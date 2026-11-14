import asyncio
import contextlib
import json
import logging
import os
import re
import sqlite3
from pathlib import Path
from datetime import datetime, timedelta, time
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
import aiosqlite
from aiogram import Bot, Dispatcher
from aiogram.filters import Command
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.exceptions import TelegramNetworkError, TelegramRetryAfter, TelegramForbiddenError, TelegramBadRequest
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage, SimpleEventIsolation
from aiogram.fsm.state import State, StatesGroup

load_dotenv()
TOKEN=os.getenv('BOT_TOKEN','').strip()
if not TOKEN or TOKEN=='replace_me': raise SystemExit('Настройка BOT_TOKEN отсутствует. Укажите токен BotFather в переменных окружения.')
raw=os.getenv('MANAGER_ID','').strip()
if not raw.isdecimal() or int(raw)<=0: raise SystemExit('MANAGER_ID должен быть числовым Telegram ID, не username. Временно укажите 1, затем узнайте свой ID командой /id.')
MANAGER=int(raw)
DB=Path(os.getenv('DB_PATH','salon.sqlite3')).expanduser().resolve()
BACKUPS=Path(os.getenv('BACKUP_DIR',str(DB.parent/'backups'))).expanduser().resolve()
TZ=ZoneInfo('Europe/Moscow')
bot=Bot(TOKEN)
dp=Dispatcher(storage=MemoryStorage(),events_isolation=SimpleEventIsolation())
class Input(State): pass
class Form(StatesGroup): text=State()

def now(): return datetime.now(TZ)
def ts(): return int(now().timestamp())
def fmt(n): return datetime.fromtimestamp(n,TZ).strftime('%d.%m.%Y %H:%M')
def kb(rows): return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=t,callback_data=c) for t,c in row] for row in rows])
async def sql(q,args=(),one=False):
    async with aiosqlite.connect(DB,timeout=30) as c:
        c.row_factory=aiosqlite.Row
        rows=await (await c.execute(q,args)).fetchall(); await c.commit()
        return (dict(rows[0]) if rows else None) if one else [dict(r) for r in rows]
async def master(uid): return await sql('SELECT * FROM masters WHERE id=?',(uid,),True)
async def send(uid,text,markup=None):
    if not uid:return 'skip'
    for attempt in range(3):
        try:
            await bot.send_message(uid,text,reply_markup=markup); return 'ok'
        except TelegramRetryAfter as e: await asyncio.sleep(min(e.retry_after,60))
        except TelegramNetworkError: await asyncio.sleep(2**attempt)
        except (TelegramForbiddenError,TelegramBadRequest):return 'permanent'
    return 'retry'

def backup_sync(label='auto'):
    BACKUPS.mkdir(parents=True,exist_ok=True)
    if not DB.exists():return
    dest=BACKUPS/(now().strftime('%Y%m%d_%H%M%S_%f')+'_'+label+'.sqlite3')
    with sqlite3.connect(DB,timeout=30) as src, sqlite3.connect(dest) as dst: src.backup(dst)
    # Keep the latest 14 daily copies; migration copies are retained separately.
    if label=='daily':
        for p in sorted(BACKUPS.glob('*_daily.sqlite3'))[:-14]:p.unlink()

async def initialize():
    DB.parent.mkdir(parents=True,exist_ok=True)
    if DB.exists():await asyncio.to_thread(backup_sync,'before_start')
    async with aiosqlite.connect(DB,timeout=30) as c:
        await c.executescript("""
        PRAGMA journal_mode=WAL;
        CREATE TABLE IF NOT EXISTS masters(id INTEGER PRIMARY KEY,name TEXT NOT NULL DEFAULT '',bio TEXT NOT NULL DEFAULT '');
        CREATE TABLE IF NOT EXISTS services(id INTEGER PRIMARY KEY AUTOINCREMENT,master INTEGER NOT NULL,name TEXT NOT NULL,minutes INTEGER NOT NULL,price TEXT,active INTEGER NOT NULL DEFAULT 1);
        CREATE TABLE IF NOT EXISTS schedule(master INTEGER,weekday INTEGER,start TEXT,end TEXT,step INTEGER,PRIMARY KEY(master,weekday));
        CREATE TABLE IF NOT EXISTS exceptions(master INTEGER,day TEXT,start TEXT,end TEXT,step INTEGER,PRIMARY KEY(master,day));
        CREATE TABLE IF NOT EXISTS bookings(id INTEGER PRIMARY KEY AUTOINCREMENT,master INTEGER NOT NULL,service INTEGER NOT NULL,service_name TEXT NOT NULL,price TEXT,start INTEGER NOT NULL,end INTEGER NOT NULL,client INTEGER,name TEXT NOT NULL,phone TEXT NOT NULL,status TEXT NOT NULL DEFAULT 'active',reminder INTEGER NOT NULL DEFAULT 0);
        CREATE INDEX IF NOT EXISTS booking_time ON bookings(master,status,start,end);
        CREATE TABLE IF NOT EXISTS v2_schedule(master INTEGER NOT NULL,target TEXT NOT NULL,intervals TEXT NOT NULL,PRIMARY KEY(master,target));
        CREATE TABLE IF NOT EXISTS v2_blocks(id INTEGER PRIMARY KEY AUTOINCREMENT,master INTEGER NOT NULL,start INTEGER NOT NULL,end INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS v2_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
        """)
        await c.execute('BEGIN IMMEDIATE')
        cols=[x[1] for x in await (await c.execute('PRAGMA table_info(bookings)')).fetchall()]
        if 'visit_state' not in cols: await c.execute("ALTER TABLE bookings ADD COLUMN visit_state TEXT NOT NULL DEFAULT 'expected'")
        if 'confirmed' not in cols: await c.execute('ALTER TABLE bookings ADD COLUMN confirmed INTEGER NOT NULL DEFAULT 0')
        done=await (await c.execute("SELECT 1 FROM v2_meta WHERE key='migrated'")).fetchone()
        if not done:
            # If the earlier updater was used, adopt its timetable first.
            exists=await (await c.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='timetable_v2'")).fetchone()
            if exists:
                await c.execute('INSERT OR IGNORE INTO v2_schedule SELECT master,key,intervals FROM timetable_v2')
            else:
                for mid,day,a,b,step in await (await c.execute('SELECT master,weekday,start,end,step FROM schedule')).fetchall():
                    await c.execute('INSERT OR IGNORE INTO v2_schedule VALUES(?,?,?)',(mid,'w:'+str(day),json.dumps([[a,b,step]])))
                for mid,day,a,b,step in await (await c.execute('SELECT master,day,start,end,step FROM exceptions')).fetchall():
                    await c.execute('INSERT OR REPLACE INTO v2_schedule VALUES(?,?,?)',(mid,'d:'+day,json.dumps([[a,b,step]] if a else [])))
            block_exists=await (await c.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='blocks_v2'")).fetchone()
            if block_exists: await c.execute('INSERT INTO v2_blocks(master,start,end) SELECT master,start,end FROM blocks_v2')
            await c.execute("INSERT INTO v2_meta VALUES('migrated','1')")
        await c.commit()
    logging.info('Database: %s; backups: %s. Persistent hosting storage must be configured separately.',DB,BACKUPS)

def unpack(text):
    data=json.loads(text)
    return [[x['start'],x['end'],x['step']] if isinstance(x,dict) else x for x in data]
async def intervals(c,mid,day):
    row=await (await c.execute('SELECT intervals FROM v2_schedule WHERE master=? AND target=?',(mid,'d:'+day.isoformat()))).fetchone()
    if row is None: row=await (await c.execute('SELECT intervals FROM v2_schedule WHERE master=? AND target=?',(mid,'w:'+str(day.weekday())))).fetchone()
    return unpack(row[0]) if row else []
def bounds(w,day):
    return tuple(int(datetime.combine(day,time.fromisoformat(t),TZ).timestamp()) for t in w[:2])
def allowed(ws,day,start,end):
    return any(bounds(w,day)[0]<=start and end<=bounds(w,day)[1] and (start-bounds(w,day)[0])%(w[2]*60)==0 for w in ws)

async def prompt(m,state,action,text,**data):
    await state.set_state(Form.text); await state.set_data(dict(action=action,**data))
    await m.answer(text+'\nИспользуйте кнопки «Назад», «Отмена» или «Главное меню».',reply_markup=NAV)
async def home(m,uid,state):
    await state.clear()
    rows=[[('Записаться','book')],[('Мои записи','my')]]
    if await master(uid): rows.append([('Кабинет мастера','staff')])
    if uid==MANAGER: rows.append([('Управление салоном','manager')])
    await m.answer('Журнал салона · версия 2 · МСК',reply_markup=kb(rows))

@dp.message(Command('start','cancel'))
async def start(m:Message,state:FSMContext): await home(m,m.from_user.id,state)
@dp.message(Command('id'))
async def identifier(m:Message): await m.answer(f'Ваш Telegram ID: {m.from_user.id}')
@dp.message(Command('privacy'))
async def privacy(m:Message):
    await m.answer(os.getenv('PRIVACY_TEXT','Для записи хранятся Telegram ID, имя, телефон и сведения о визитах. Доступ имеют управляющий и соответствующий мастер. По вопросам обработки и удаления данных обратитесь к салону. До коммерческого запуска владелец должен заменить этот текст своей политикой и контактами через PRIVACY_TEXT.'))

async def select_masters(m,state,manual=False):
    await state.clear(); await state.update_data(manual=manual)
    rows=await sql("SELECT * FROM masters WHERE name<>'' ORDER BY name")
    if not rows:return await m.answer('Пока нет мастеров с заполненным профилем.')
    for i in range(0,len(rows),30):await m.answer('Выберите мастера:',reply_markup=kb([[(r['name'],f"master:{r['id']}")] for r in rows[i:i+30]]))
async def select_services(m,state,mid):
    p=await master(mid)
    if not p or not p['name']:return await m.answer('Сначала заполните профиль мастера.')
    await state.update_data(mid=mid)
    rows=await sql('SELECT * FROM services WHERE master=? AND active=1',(mid,))
    if not rows:return await m.answer('У мастера пока нет услуг.')
    await m.answer(p['name']+'\n'+p['bio'])
    for i in range(0,len(rows),30):
        await m.answer('Услуги:',reply_markup=kb([[(r['name']+f" · {r['minutes']} мин"+(f" · {r['price']} ₽" if r['price'] else ''),f"service:{r['id']}")] for r in rows[i:i+30]]))
async def dates(m,state):
    await state.set_state(Form.text); await state.update_data(action='date')
    await m.answer('Выберите день или введите дату ДД.ММ.ГГГГ:',reply_markup=kb([[('Сегодня','day:0'),('Завтра','day:1')],[('В меню','home')]]))
async def service_for(d):
    if d.get('move'):
        b=await sql("SELECT * FROM bookings WHERE id=? AND status='active' AND visit_state='expected'",(d['move'],),True)
        if not b:return None
        return dict(id=b['service'],master=b['master'],name=b['service_name'],minutes=(b['end']-b['start'])//60,price=b['price'])
    return await sql('SELECT * FROM services WHERE id=? AND active=1',(d.get('sid',0),),True)
async def slots(m,state,day):
    d=await state.get_data(); s=await service_for(d)
    if not s:return await m.answer('Услуга недоступна. /start')
    if not now().date()<=day<=now().date()+timedelta(days=365):return await m.answer('Дата должна быть в пределах 365 дней от сегодня.')
    async with aiosqlite.connect(DB) as c:ws=await intervals(c,s['master'],day)
    if not ws:return await m.answer('Выходной. Введите другую дату.')
    left=int(datetime.combine(day,time.min,TZ).timestamp()); right=left+86400
    busy=await sql("SELECT start,end FROM bookings WHERE master=? AND status='active' AND start<? AND end>? AND id<>?",(s['master'],right,left,d.get('move',0)))
    busy+=await sql('SELECT start,end FROM v2_blocks WHERE master=? AND start<? AND end>?',(s['master'],right,left))
    available=set()
    for w in ws:
        a,b=bounds(w,day)
        for t in range(a,b,w[2]*60):
            end=t+s['minutes']*60
            if t>ts() and end<=b and not any(t<x['end'] and end>x['start'] for x in busy):available.add(t)
    await state.update_data(action='date',sid=s['id'])
    if not available:return await m.answer('Свободных окон нет. Введите другую дату.')
    values=sorted(available)
    for i in range(0,len(values),40):await m.answer('Время '+day.strftime('%d.%m.%Y'),reply_markup=kb([[(datetime.fromtimestamp(t,TZ).strftime('%H:%M'),f"time:{s['id']}:{t}")] for t in values[i:i+40]]))

def details(b):
    status='Отменён' if b['status']=='cancelled' else {'expected':'Ожидается','arrived':'Пришёл','missed':'Не пришёл'}.get(b['visit_state'],'Ожидается')
    return f"Запись №{b['id']} · {fmt(b['start'])} МСК\n{b['service_name']}\nИмя: {b['name']}\nТелефон: {b['phone']}\nЦена: {b['price']+' ₽' if b['price'] else 'не указана'}\nСтатус: {status}\nКлиент подтвердил: {'да' if b['confirmed'] else 'нет'}"
def client_buttons(b):
    if b['status']!='active' or b['visit_state']!='expected' or b['start']<=ts():return None
    return kb([[('Приду',f"yes:{b['id']}:{b['start']}")],[('Перенести',f"move:{b['id']}"),('Отменить',f"cancel:{b['id']}")]])
def staff_buttons(b):
    if b['status']!='active':return None
    rows=[]
    if b['start']>ts() and b['visit_state']=='expected':rows.append([('Перенести',f"move:{b['id']}"),('Отменить',f"cancel:{b['id']}")])
    if b['start']<=ts():rows.extend([[('Пришёл',f"status:{b['id']}:arrived"),('Не пришёл',f"status:{b['id']}:missed")],[('Сбросить отметку',f"status:{b['id']}:expected")]])
    return kb(rows) if rows else None
async def confirm(m,state):
    d=await state.get_data(); s=await service_for(d)
    if not s:return await m.answer('Запись недоступна. /start')
    p=await sql('SELECT * FROM masters WHERE id=?',(s['master'],),True)
    await state.set_state(None); await state.update_data(action='confirm')
    await m.answer(f"Подтвердите запись:\n{p['name']} · {s['name']} · {s['minutes']} мин\nЦена: {s['price']+' ₽' if s['price'] else 'не указана'}\n{fmt(d['time'])} МСК\n{d['name']} · {d['phone']}\n\nДанные передаются салону для оформления записи. /privacy",reply_markup=kb([[('Подтвердить','confirm')],[('В меню','home')]]))
async def save(uid,d):
    async with aiosqlite.connect(DB,timeout=30) as c:
        c.row_factory=aiosqlite.Row; await c.execute('BEGIN IMMEDIATE')
        old=None
        if d.get('move'):
            old=await (await c.execute("SELECT * FROM bookings WHERE id=? AND status='active' AND visit_state='expected' AND start>?",(d['move'],ts()))).fetchone()
            if not old or uid not in (old['client'],old['master'],MANAGER):raise ValueError('Нет доступа к переносу.')
            if old['start']!=d['original']:raise ValueError('Запись уже изменена. Откройте её заново через /start.')
            s=dict(id=old['service'],master=old['master'],name=old['service_name'],price=old['price'],minutes=(old['end']-old['start'])//60)
        else:
            s=await (await c.execute('SELECT * FROM services WHERE id=? AND active=1',(d['sid'],))).fetchone()
            if not s:raise ValueError('Услуга недоступна.')
        enabled_row=await (await c.execute('SELECT enabled FROM masters WHERE id=?',(s['master'],))).fetchone()
        active_master=bool(enabled_row and enabled_row['enabled'])
        if not old and not active_master:raise ValueError('Мастер недоступен для новых записей.')
        if old and uid!=MANAGER and uid!=old['client'] and not active_master:raise ValueError('Права мастера отозваны.')
        if d.get('manual') and uid!=MANAGER and (uid!=s['master'] or not active_master):raise ValueError('Нет прав.')
        t=d['time']; end=t+s['minutes']*60; day=datetime.fromtimestamp(t,TZ).date()
        if t<=ts() or day>now().date()+timedelta(days=365):raise ValueError('Выберите будущую дату в пределах года.')
        if not allowed(await intervals(c,s['master'],day),day,t,end):raise ValueError('График изменился. Выберите время заново.')
        clash=await (await c.execute("SELECT 1 FROM bookings WHERE master=? AND status='active' AND start<? AND end>? AND id<>?",(s['master'],end,t,d.get('move',0)))).fetchone()
        blocked=await (await c.execute('SELECT 1 FROM v2_blocks WHERE master=? AND start<? AND end>?',(s['master'],end,t))).fetchone()
        if clash or blocked:raise ValueError('Это время уже занято или закрыто.')
        rem=int(t-ts()<86400)
        if old:
            await c.execute('UPDATE bookings SET start=?,end=?,reminder=?,confirmed=0 WHERE id=?',(t,end,rem,old['id'])); bid=old['id']
        else:
            cur=await c.execute('INSERT INTO bookings(master,service,service_name,price,start,end,client,name,phone,reminder) VALUES(?,?,?,?,?,?,?,?,?,?)',(s['master'],s['id'],s['name'],s['price'],t,end,None if d.get('manual') else uid,d['name'],d['phone'],rem));bid=cur.lastrowid
        await c.commit();return bid

async def listing(m,uid,scope,period):
    if scope=='manager' and uid!=MANAGER:return
    if scope=='staff' and not await master(uid):return
    base='SELECT b.*,m.name AS master_name FROM bookings b JOIN masters m ON m.id=b.master WHERE '
    if scope=='client':q=base+"b.client=? AND b.status='active' AND b.start>?"; args=[uid,ts()]
    elif period=='all':q=base+"b.status='active' AND b.start>?";args=[ts()]
    else:
        day=now().date()+timedelta(days=1 if period=='tomorrow' else 0)
        count={'week':7,'month':30}.get(period,1)
        a=int(datetime.combine(day,time.min,TZ).timestamp());b=a+count*86400
        q=base+'b.start>=? AND b.start<?';args=[a,b]
    if scope=='staff':q+=' AND b.master=?';args.append(uid)
    rows=await sql(q+' ORDER BY b.start',args)
    await m.answer(f'Записей: {len(rows)}')
    for b in rows:
        await m.answer('Мастер: '+b['master_name']+'\n'+details(b),reply_markup=client_buttons(b) if scope=='client' else staff_buttons(b));await asyncio.sleep(.06)

async def schedule_menu(m,state,uid):
    await state.clear()
    names=['Пн','Вт','Ср','Чт','Пт','Сб','Вс']
    lines=['График МСК. Неуказанные дни — выходные.']
    for r in await sql('SELECT * FROM v2_schedule WHERE master=? ORDER BY target',(uid,)):
        k=r['target']
        if k.startswith('d:') and k[2:]<now().date().isoformat():continue
        title=names[int(k[2:])] if k.startswith('w:') else k[2:]
        lines.append(title+': '+('; '.join(f'{a}–{b}, шаг {s} мин' for a,b,s in unpack(r['intervals'])) or 'выходной'))
    text='\n'.join(lines)
    for i in range(0,len(text),3500):await m.answer(text[i:i+3500])
    await m.answer('Рабочие интервалы разделяются перерывами.',reply_markup=kb([[('Дни недели','week')],[('Конкретная дата','schedule_date')],[('Закрыть личное время','block_date')],[('Закрытые интервалы','blocks')],[('В кабинет','staff')]]))
async def target_menu(m,state,uid,target):
    await state.set_state(None);await state.set_data(dict(action='target',target=target))
    r=await sql('SELECT intervals FROM v2_schedule WHERE master=? AND target=?',(uid,target),True)
    ws=unpack(r['intervals']) if r else []
    text='Интервалы: '+('; '.join(f'{a}–{b} / {s} мин' for a,b,s in ws) or 'не заданы')
    if target.startswith('d:') and not r:text='Используется недельный график. Добавление создаст отдельный график этой даты.'
    rows=[[('Добавить интервал','interval_add')],[('Сделать выходным','off')]]
    for i,w in enumerate(ws):rows.append([(f'Удалить {w[0]}–{w[1]}',f'interval_del:{i}')])
    if target.startswith('d:'):rows.append([('Вернуть недельный график','reset')])
    rows.append([('К расписанию','schedule')])
    await m.answer(text,reply_markup=kb(rows))
async def clock(m,state,which):
    await state.set_state(Form.text);await state.update_data(action='clock',which=which)
    rows=[];cells=[]
    for minute in range(0,1440,30):
        label=f'{minute//60:02d}:{minute%60:02d}';cells.append((label,'clock:'+label))
        if len(cells)==4:rows.append(cells);cells=[]
    await m.answer(('Начало' if which=='start' else 'Конец')+' интервала: выберите кнопку или введите ЧЧ:ММ. Смена должна закончиться до полуночи.',reply_markup=kb(rows))
async def clock_value(m,state,value):
    if not re.fullmatch(r'\d{2}:\d{2}',value):raise ValueError('Введите время ЧЧ:ММ.')
    t=time.fromisoformat(value);d=await state.get_data()
    if d['which']=='start':
        await state.update_data(start=value);return await clock(m,state,'end')
    if value<=d['start']:raise ValueError('Конец должен быть позже начала в тот же день.')
    await state.update_data(end=value);await state.set_state(None)
    if d.get('block'):
        await state.update_data(action='block_confirm')
        return await m.answer(f"Закрыть {d['day']} {d['start']}–{value}? Существующие записи не отменяются.",reply_markup=kb([[('Закрыть','block_save')],[('Отмена','schedule')]]))
    await state.update_data(action='step')
    await m.answer('Шаг начала записей:',reply_markup=kb([[(f'{n} мин',f'step:{n}') for n in (5,10,15)],[(f'{n} мин',f'step:{n}') for n in (20,30,60)]]))
async def conflicts(m,uid):
    rows=await sql("SELECT * FROM bookings WHERE master=? AND status='active' AND visit_state='expected' AND end>?",(uid,ts()));bad=[]
    async with aiosqlite.connect(DB) as c:
        for b in rows:
            day=datetime.fromtimestamp(b['start'],TZ).date();ws=await intervals(c,uid,day)
            contained=any(bounds(w,day)[0]<=b['start'] and b['end']<=bounds(w,day)[1] for w in ws)
            blocked=await (await c.execute('SELECT 1 FROM v2_blocks WHERE master=? AND start<? AND end>?',(uid,b['end'],b['start']))).fetchone()
            if not contained or blocked:bad.append(str(b['id']))
    if bad:await m.answer('ВНИМАНИЕ: записи № '+', '.join(bad)[:3000]+' конфликтуют с графиком, но НЕ отменены. Свяжитесь с клиентами.')

@dp.callback_query()
async def callbacks(q:CallbackQuery,state:FSMContext):
    await q.answer();m=q.message;uid=q.from_user.id;k=q.data
    try:await dispatch(m,uid,k,state)
    except (ValueError,OverflowError) as e:await m.answer(str(e)+'\nПовторите действие или /start.')

async def dispatch(m,uid,k,state):
    d=await state.get_data()
    if k=='home':return await home(m,uid,state)
    if k=='book':return await select_masters(m,state)
    if k=='my':return await listing(m,uid,'client','all')
    if k in ('staff','manager'):
        if k=='manager' and uid!=MANAGER:return
        if k=='staff' and not await master(uid):return
        await state.clear()
        rows=([[('Выдать права мастера','grant')],[('Ручная запись','manual_all')]] if k=='manager' else [[('Профиль','profile'),('Услуги','services')],[('Расписание','schedule')],[('Ручная запись','manual_self')]])
        rows += [[('Сегодня',f'list:{k}:today'),('Завтра',f'list:{k}:tomorrow')],[('7 дней',f'list:{k}:week'),('30 дней',f'list:{k}:month')],[('Все предстоящие',f'list:{k}:all')],[('В меню','home')]]
        return await m.answer('Управление салоном' if k=='manager' else 'Кабинет мастера',reply_markup=kb(rows))
    if k=='grant':
        if uid==MANAGER:await prompt(m,state,'grant','Введите числовой Telegram ID мастера. Мастер узнает его командой /id.')
        return
    if k=='manual_all':
        if uid==MANAGER:await select_masters(m,state,True)
        return
    if k=='manual_self':
        if not await master(uid):return
        await state.clear();await state.update_data(manual=True);return await select_services(m,state,uid)
    if k.startswith('list:'):
        _,scope,period=k.split(':')
        if scope not in ('manager','staff'):return
        return await listing(m,uid,scope,period)
    if k.startswith('master:'):
        mid=int(k.split(':')[1])
        if d.get('manual') and uid not in (mid,MANAGER):return
        return await select_services(m,state,mid)
    if k.startswith('service:'):
        sid=int(k.split(':')[1]);s=await sql('SELECT * FROM services WHERE id=? AND active=1',(sid,),True)
        if not s or s['master']!=d.get('mid'):raise ValueError('Устаревшее меню.')
        await state.update_data(sid=sid);return await dates(m,state)
    if k.startswith('day:'):
        if d.get('action')!='date':raise ValueError('Устаревшее меню.')
        return await slots(m,state,now().date()+timedelta(days=int(k.split(':')[1])))
    if k.startswith('time:'):
        _,sid,t=k.split(':')
        if d.get('sid')!=int(sid) or d.get('action')!='date':raise ValueError('Устаревшее меню.')
        await state.update_data(time=int(t))
        if d.get('move'):return await confirm(m,state)
        await state.set_state(Form.text);await state.update_data(action='name');return await m.answer('Введите имя клиента:')
    if k=='confirm':
        if d.get('action')!='confirm':raise ValueError('Запись уже обработана или меню устарело.')
        try:bid=await save(uid,d)
        except ValueError as e:
            await m.answer(str(e));return await dates(m,state)
        b=await sql('SELECT * FROM bookings WHERE id=?',(bid,),True);await state.clear()
        title='Запись перенесена' if d.get('move') else 'Запись оформлена'
        await m.answer(title+'\n'+details(b),reply_markup=client_buttons(b) if b['client']==uid else staff_buttons(b))
        for recipient in {b['master'],b['client']}-{None,uid}:
            await send(recipient,title+'\n'+details(b),client_buttons(b) if recipient==b['client'] else staff_buttons(b))
        return
    if k.startswith(('move:','cancel:','cancelok:','yes:','status:')):
        p=k.split(':');action=p[0];bid=int(p[1]);b=await sql('SELECT * FROM bookings WHERE id=?',(bid,),True)
        if not b or uid not in (b['client'],b['master'],MANAGER):raise ValueError('Запись недоступна.')
        if action=='status':
            if uid not in (b['master'],MANAGER):return
            value=p[2]
            if value not in ('expected','arrived','missed'):return
            changed=await sql("UPDATE bookings SET visit_state=? WHERE id=? AND status='active' AND start<=? RETURNING id",(value,bid,ts()))
            if not changed:raise ValueError('Отметка доступна с начала неотменённого визита.')
            b=await sql('SELECT * FROM bookings WHERE id=?',(bid,),True);return await m.answer(details(b),reply_markup=staff_buttons(b))
        if b['status']!='active' or b['visit_state']!='expected' or b['start']<=ts():raise ValueError('Эта запись уже недоступна для изменения.')
        if action=='yes':
            if uid!=b['client']:return
            if int(p[2])!=b['start']:raise ValueError('Время изменилось. Откройте «Мои записи».')
            updated=await sql("UPDATE bookings SET confirmed=1 WHERE id=? AND start=? AND status='active' AND visit_state='expected' AND confirmed=0 AND start>? RETURNING id",(bid,b['start'],ts()))
            await m.answer('Спасибо! Визит подтверждён.')
            if updated:await send(b['master'],f"Клиент подтвердил визит №{bid}: {fmt(b['start'])}")
            return
        if action=='cancel':return await m.answer('Отменить запись?\n'+details(b),reply_markup=kb([[('Да, отменить',f"cancelok:{bid}:{b['start']}")],[('Назад','home')]]))
        if action=='cancelok':
            if len(p)!=3 or int(p[2])!=b['start']:raise ValueError('Запись уже изменена. Откройте её заново.')
            changed=await sql("UPDATE bookings SET status='cancelled' WHERE id=? AND start=? AND status='active' AND visit_state='expected' AND start>? RETURNING id",(bid,b['start'],ts()))
            if not changed:raise ValueError('Запись уже изменена.')
            await m.answer('Запись отменена, время освобождено.')
            for recipient in {b['master'],b['client']}-{None,uid}:await send(recipient,'Запись отменена\n'+details(dict(b,status='cancelled')))
            return
        await state.clear();await state.update_data(move=bid,original=b['start'],sid=b['service'],mid=b['master'],name=b['name'],phone=b['phone'],manual=False)
        return await dates(m,state)
    # All remaining actions are available to masters only.
    if not await master(uid):return
    if k=='profile':return await prompt(m,state,'profile','Имя | описание\nНапример: Анна | Мастер маникюра')
    if k=='services':
        for s in await sql('SELECT * FROM services WHERE master=? AND active=1',(uid,)):
            await m.answer(f"{s['name']} · {s['minutes']} мин · {s['price'] or 'без цены'}",reply_markup=kb([[('Редактировать',f"edit:{s['id']}"),('Убрать',f"hide:{s['id']}")]]))
        return await m.answer('Услуги',reply_markup=kb([[('Добавить','addservice')],[('В кабинет','staff')]]))
    if k=='addservice' or k.startswith('edit:'):
        sid=int(k.split(':')[1]) if ':' in k else 0
        if sid and not await sql('SELECT id FROM services WHERE id=? AND master=?',(sid,uid),True):return
        return await prompt(m,state,'service','Название | минуты | цена или -\nНапример: Стрижка | 60 | 1500',sid=sid)
    if k.startswith('hide:'):
        await sql('UPDATE services SET active=0 WHERE id=? AND master=?',(int(k.split(':')[1]),uid));return await m.answer('Услуга скрыта. Уже оформленные записи сохранены.')
    if k=='schedule':return await schedule_menu(m,state,uid)
    if k=='week':return await m.answer('Выберите день:',reply_markup=kb([[(name,f'weekday:{i}')] for i,name in enumerate(['Понедельник','Вторник','Среда','Четверг','Пятница','Суббота','Воскресенье'])]))
    if k.startswith('weekday:'):
        n=int(k.split(':')[1])
        if n not in range(7):return
        return await target_menu(m,state,uid,'w:'+str(n))
    if k in ('schedule_date','block_date'):return await prompt(m,state,k,'Введите дату ДД.ММ.ГГГГ:')
    if k=='blocks':
        rows=await sql('SELECT * FROM v2_blocks WHERE master=? AND end>? ORDER BY start',(uid,ts()))
        if not rows:await m.answer('Нет закрытых интервалов.')
        for r in rows:await m.answer(fmt(r['start'])+' — '+fmt(r['end']),reply_markup=kb([[('Открыть время',f"unblock:{r['id']}")]]))
        return
    if k.startswith('unblock:'):
        await sql('DELETE FROM v2_blocks WHERE id=? AND master=?',(int(k.split(':')[1]),uid));return await m.answer('Интервал открыт.')
    if k=='interval_add':
        if d.get('action')!='target':raise ValueError('Откройте день заново.')
        await state.update_data(block=False);return await clock(m,state,'start')
    if k.startswith('clock:'):
        if d.get('action')!='clock':raise ValueError('Откройте настройку заново.')
        return await clock_value(m,state,k[6:])
    if k=='block_save':
        if d.get('action')!='block_confirm':raise ValueError('Форма устарела.')
        day=datetime.strptime(d['day'],'%d.%m.%Y').date();a,b=bounds([d['start'],d['end']],day)
        await sql('INSERT INTO v2_blocks(master,start,end) VALUES(?,?,?)',(uid,a,b));await state.clear();await m.answer('Время закрыто для новых записей.');return await conflicts(m,uid)
    if k.startswith('step:'):
        if d.get('action')!='step':raise ValueError('Откройте настройку заново.')
        step=int(k.split(':')[1])
        if step not in (5,10,15,20,30,60):return
        r=await sql('SELECT intervals FROM v2_schedule WHERE master=? AND target=?',(uid,d['target']),True);ws=unpack(r['intervals']) if r else []
        if any(d['start']<w[1] and d['end']>w[0] for w in ws):raise ValueError('Интервал пересекает существующий. Удалите старый или добавьте непересекающийся.')
        ws.append([d['start'],d['end'],step]);ws.sort()
        await sql('INSERT OR REPLACE INTO v2_schedule VALUES(?,?,?)',(uid,d['target'],json.dumps(ws)))
        await target_menu(m,state,uid,d['target']);return await conflicts(m,uid)
    if k in ('off','reset') or k.startswith('interval_del:'):
        if d.get('action')!='target':raise ValueError('Откройте день заново.')
        target=d['target']
        if k=='reset':
            if not target.startswith('d:'):return
            await sql('DELETE FROM v2_schedule WHERE master=? AND target=?',(uid,target))
        else:
            ws=[]
            if k.startswith('interval_del:'):
                r=await sql('SELECT intervals FROM v2_schedule WHERE master=? AND target=?',(uid,target),True);ws=unpack(r['intervals']) if r else []
                idx=int(k.split(':')[1])
                if not 0<=idx<len(ws):raise ValueError('Список изменился.')
                ws.pop(idx)
            await sql('INSERT OR REPLACE INTO v2_schedule VALUES(?,?,?)',(uid,target,json.dumps(ws)))
        await target_menu(m,state,uid,target);return await conflicts(m,uid)

async def input_text(m:Message,state:FSMContext):
    d=await state.get_data();action=d.get('action');uid=m.from_user.id;text=(m.text or '').strip()
    try:
        if action=='grant':
            if uid!=MANAGER:return
            if not text.isdecimal() or int(text)<=0:raise ValueError('Введите числовой Telegram ID.')
            await sql('INSERT OR IGNORE INTO masters(id) VALUES(?)',(int(text),));await send(int(text),'Вам выданы права мастера. Откройте /start → Кабинет мастера.');await state.clear();return await m.answer('Права выданы.')
        if action in ('profile','service','schedule_date','block_date','clock') and not await master(uid):return
        if action=='profile':
            parts=text.split('|',1);name=parts[0].strip();bio=parts[1].strip() if len(parts)>1 else ''
            if not 1<=len(name)<=80 or len(bio)>500:raise ValueError('Имя 1–80 символов, описание до 500.')
            await sql('UPDATE masters SET name=?,bio=? WHERE id=?',(name,bio,uid))
        elif action=='service':
            p=[x.strip() for x in text.split('|')]
            if len(p) not in (2,3):raise ValueError('Формат: название | минуты | цена или -')
            name=p[0];minutes=int(p[1]);price=p[2].replace(',','.') if len(p)==3 and p[2]!='-' else None
            if not 1<=len(name)<=80 or not 5<=minutes<=720:raise ValueError('Название до 80 символов, длительность 5–720 минут.')
            if price is not None and not re.fullmatch(r'\d{1,7}(\.\d{1,2})?',price):raise ValueError('Цена должна быть числом или -.')
            if d['sid']:await sql('UPDATE services SET name=?,minutes=?,price=? WHERE id=? AND master=?',(name,minutes,price,d['sid'],uid))
            else:await sql('INSERT INTO services(master,name,minutes,price) VALUES(?,?,?,?)',(uid,name,minutes,price))
        elif action in ('schedule_date','block_date'):
            day=datetime.strptime(text,'%d.%m.%Y').date()
            if not now().date()<=day<=now().date()+timedelta(days=365):raise ValueError('Выберите дату в пределах ближайшего года.')
            if action=='schedule_date':return await target_menu(m,state,uid,'d:'+day.isoformat())
            await state.update_data(block=True,day=day.strftime('%d.%m.%Y'));return await clock(m,state,'start')
        elif action=='clock':return await clock_value(m,state,text)
        elif action=='date':return await slots(m,state,datetime.strptime(text,'%d.%m.%Y').date())
        elif action=='name':
            if not 1<=len(text)<=100:raise ValueError('Имя должно содержать 1–100 символов.')
            await state.update_data(name=text,action='phone');return await m.answer('Введите телефон клиента, например +79991234567. Чужие данные вводите только с разрешения клиента.')
        elif action=='phone':
            value=m.contact.phone_number if m.contact else text;value=re.sub(r'[\s()\-]','',value)
            if not re.fullmatch(r'\+?\d{10,15}',value):raise ValueError('Номер должен содержать 10–15 цифр.')
            await state.update_data(phone=value);return await confirm(m,state)
        else:return await m.answer('Откройте /start.')
        await state.clear();await m.answer('Сохранено. /start — меню.')
    except (ValueError,OverflowError) as e:await m.answer('Проверьте ввод: '+str(e)+'\nПовторите или /start.')

async def fallback(m:Message):await m.answer('Главное меню: /start. Ваш ID: /id.')

async def reminders():
    while True:
        try:
            rows=await sql("SELECT * FROM bookings WHERE status='active' AND visit_state='expected' AND reminder=0 AND start>? AND start<=?",(ts(),ts()+86400))
            for b in rows:
                current=await sql("SELECT * FROM bookings WHERE id=? AND start=? AND reminder=0 AND status='active' AND visit_state='expected'",(b['id'],b['start']),True)
                if not current:continue
                result=await send(b['client'],'Напоминание о визите\n'+details(current),client_buttons(current))
                if result!='retry':
                    await sql('UPDATE bookings SET reminder=1 WHERE id=? AND start=?',(b['id'],b['start']))
                    if result=='permanent':await send(b['master'],f"Не удалось доставить напоминание клиенту записи №{b['id']}. Свяжитесь самостоятельно.")
                await asyncio.sleep(.1)
        except Exception:logging.exception('Reminder error')
        await asyncio.sleep(30)
async def backups():
    while True:
        try:await asyncio.to_thread(backup_sync,'daily')
        except Exception:logging.exception('BACKUP FAILED — check disk and permissions')
        await asyncio.sleep(86400)
async def main():
    logging.basicConfig(level=logging.INFO)
    jobs=[]
    try:
        await initialize()
        while True:
            try:
                await bot.get_me();break
            except (TelegramNetworkError,TelegramRetryAfter):
                logging.warning('Telegram unavailable. Retry in 15 seconds.');await asyncio.sleep(15)
        jobs=[asyncio.create_task(reminders()),asyncio.create_task(backups())]
        await dp.start_polling(bot,close_bot_session=False)
    finally:
        for job in jobs:job.cancel()
        for job in jobs:
            with contextlib.suppress(asyncio.CancelledError):await job
        await bot.session.close()

# ---- Salon v3 extension: compatible with the supplied version 2 ----
import calendar
import secrets
from aiogram import F
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton

NAV=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text='Главное меню'),KeyboardButton(text='Назад'),KeyboardButton(text='Отмена')]],resize_keyboard=True,is_persistent=True)
_initialize2=initialize
_dispatch2=dispatch
_input2=input_text
_save2=save
_details2=details
_staffbuttons2=staff_buttons
_target2=target_menu
_clock2=clock
_schedule2=schedule_menu
_confirm2=confirm
_selectservices2=select_services

async def initialize():
    await _initialize2()
    async with aiosqlite.connect(DB,timeout=30) as c:
        await c.execute('BEGIN IMMEDIATE')
        cols=[x[1] for x in await (await c.execute('PRAGMA table_info(masters)')).fetchall()]
        if 'enabled' not in cols:await c.execute('ALTER TABLE masters ADD COLUMN enabled INTEGER NOT NULL DEFAULT 1')
        await c.commit()

async def master(uid):
    return await sql('SELECT * FROM masters WHERE id=? AND enabled=1',(uid,),True)

async def ready(mid):
    p=await master(mid)
    if not p:return False,False,False
    service=await sql('SELECT id FROM services WHERE master=? AND active=1 LIMIT 1',(mid,),True)
    rows=await sql('SELECT * FROM v2_schedule WHERE master=?',(mid,))
    schedule=any(unpack(r['intervals']) and (r['target'].startswith('w:') or r['target'][2:]>=now().date().isoformat()) for r in rows)
    return bool(p['name'].strip()),bool(service),schedule

async def home(m,uid,state):
    await state.clear()
    p=await master(uid)
    title='CRM Салона' if p or uid==MANAGER else 'Запись к мастеру'
    await m.answer(title,reply_markup=NAV)
    rows=[[('Записаться','book'),('Мои записи','my')]]
    if p:rows.append([('Кабинет мастера','staff')])
    if uid==MANAGER:rows.append([('Управление салоном','manager')])
    await m.answer('Выберите действие:',reply_markup=kb(rows))

async def select_masters(m,state,manual=False):
    await state.clear();await state.update_data(manual=manual)
    items=[]
    for p in await sql('SELECT * FROM masters WHERE enabled=1 ORDER BY name'):
        if all(await ready(p['id'])):items.append(p)
    if not items:return await m.answer('Пока нет мастеров, готовых к записи.',reply_markup=kb([[('Главное меню','home')]]))
    for i in range(0,len(items),30):
        await m.answer('Выберите мастера:',reply_markup=kb([[(p['name'],f"master:{p['id']}")] for p in items[i:i+30]]))

async def select_services(m,state,mid):
    if not await master(mid):return await m.answer('Мастер сейчас недоступен для новых записей.')
    await _selectservices2(m,state,mid)

async def save(uid,d):
    # Final active-role checks are also added inside the original transaction.
    return await _save2(uid,d)

def details(b):
    return _details2(b)+f"\nПродолжительность: {(b['end']-b['start'])//60} мин"

def staff_buttons(b):
    old=_staffbuttons2(b)
    rows=[] if old is None else list(old.inline_keyboard)
    rows.append([InlineKeyboardButton(text='Имя и телефон',callback_data=f"v3:contacts:{b['id']}")])
    rows.append([InlineKeyboardButton(text='Записать повторно',callback_data=f"v3:repeat:{b['id']}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

async def access_booking(uid,b,staff=False):
    if not b:return False
    if uid==MANAGER:return True
    if uid==b['master'] and await master(uid):return True
    return not staff and uid==b['client']

async def card(m,uid,bid):
    b=await sql('SELECT b.*,m.name AS master_name FROM bookings b JOIN masters m ON m.id=b.master WHERE b.id=?',(bid,),True)
    if not await access_booking(uid,b):raise ValueError('Нет доступа к записи.')
    markup=staff_buttons(b) if await access_booking(uid,b,True) else client_buttons(b)
    await m.answer('Мастер: '+b['master_name']+'\n'+details(b),reply_markup=markup)

async def cabinet(m,uid,state,kind):
    if kind=='manager' and uid!=MANAGER:return
    if kind=='staff' and not await master(uid):return
    await state.clear()
    if kind=='manager':
        rows=[[('Мастера','v3:masters'),('Выдать права','grant')],[('Записать клиента','manual_all')]]
    else:
        rows=[[('Профиль','profile'),('Услуги','services')],[('Расписание','schedule'),('Записать клиента','manual_self')],[('Проверка готовности','v3:ready')]]
    rows += [[('Сегодня',f'list:{kind}:today'),('Завтра',f'list:{kind}:tomorrow')],[('7 дней',f'list:{kind}:week'),('30 дней',f'list:{kind}:month')],[('Все предстоящие',f'list:{kind}:all')],[('История',f'v3:history:{kind}:0'),('Поиск клиента','v3:search')],[('Главное меню','home')]]
    await m.answer('CRM Салона',reply_markup=kb(rows))

async def readiness(m,mid):
    flags=await ready(mid)
    names=['Профиль заполнен','Услуги добавлены','Расписание задано']
    await m.answer('\n'.join(('✅ ' if flag else '❌ ')+name for flag,name in zip(flags,names)),reply_markup=kb([[('Профиль','profile'),('Услуги','services')],[('Расписание','schedule'),('В кабинет','staff')]]))

async def master_card(m,mid):
    p=await sql('SELECT * FROM masters WHERE id=?',(mid,),True)
    if not p:raise ValueError('Мастер не найден.')
    flags=await ready(mid)
    count=await sql("SELECT COUNT(*) AS n FROM bookings WHERE master=? AND status='active' AND start>?",(mid,ts()),True)
    near=await sql("SELECT start FROM bookings WHERE master=? AND status='active' AND start>? ORDER BY start LIMIT 1",(mid,ts()),True)
    text=f"Мастер: {p['name'] or 'Профиль не заполнен'}\nID: {mid}\n{p['bio']}\nПрава: {'активны' if p['enabled'] else 'отозваны'}\nПредстоящих записей: {count['n']}\nБлижайшая: {fmt(near['start']) if near else 'нет'}"
    if p['enabled']:text+='\nГотовность: '+('готов к записи' if all(flags) else 'нужно завершить настройку')
    rows=[[('Записи',f'v3:masterbook:{mid}:0'),('Расписание',f'v3:masterschedule:{mid}')]]
    if p['enabled']:rows.append([('Записать клиента',f'v3:manual:{mid}')])
    rows.extend([[('Отозвать права' if p['enabled'] else 'Восстановить права',f"v3:right:{mid}:{0 if p['enabled'] else 1}")],[('Все мастера','v3:masters')]])
    await m.answer(text,reply_markup=kb(rows))

async def history(m,uid,state,scope,page=0):
    if uid!=MANAGER and not await master(uid):raise ValueError('Нет доступа.')
    filters=[];args=[]
    if uid!=MANAGER:filters.append('b.master=?');args.append(uid)
    if scope.startswith('master-'):
        if uid!=MANAGER:raise ValueError('Нет доступа.')
        filters.append('b.master=?');args.append(int(scope[7:]))
    elif scope=='search':
        d=await state.get_data();term=d.get('search','')
        if not term:raise ValueError('Повторите поиск.')
        # Literal substring match: SQL wildcards in names have no special meaning.
        filters.append('(instr(lower(b.name),lower(?))>0 OR instr(b.phone,?)>0)');args.extend([term,term])
    base=' FROM bookings b JOIN masters m ON m.id=b.master'+(' WHERE '+' AND '.join(filters) if filters else '')
    count=await sql('SELECT COUNT(*) AS n'+base,args,True)
    page=max(0,page)
    rows=await sql('SELECT b.*,m.name AS master_name'+base+' ORDER BY b.start DESC LIMIT 8 OFFSET ?',args+[page*8])
    await m.answer(f"Найдено: {count['n']}. Страница {page+1}.")
    for b in rows:
        await m.answer('Мастер: '+b['master_name']+'\n'+details(b),reply_markup=staff_buttons(b));await asyncio.sleep(.06)
    buttons=[]
    if page:buttons.append(('←',f'v3:history:{scope}:{page-1}'))
    if (page+1)*8<count['n']:buttons.append(('→',f'v3:history:{scope}:{page+1}'))
    await m.answer('Навигация:',reply_markup=kb(([buttons] if buttons else [])+[[('В кабинет','manager' if uid==MANAGER else 'staff')]]))

async def availability(s,day,move=0):
    async with aiosqlite.connect(DB) as c:ws=await intervals(c,s['master'],day)
    if not ws:return 'off'
    left=int(datetime.combine(day,time.min,TZ).timestamp());right=left+86400
    busy=await sql("SELECT start,end FROM bookings WHERE master=? AND status='active' AND start<? AND end>? AND id<>?",(s['master'],right,left,move))
    busy+=await sql('SELECT start,end FROM v2_blocks WHERE master=? AND start<? AND end>?',(s['master'],right,left))
    for w in ws:
        a,b=bounds(w,day)
        for t in range(a,b,w[2]*60):
            end=t+s['minutes']*60
            if t>ts() and end<=b and not any(t<x['end'] and end>x['start'] for x in busy):return 'free'
    return 'full'

async def dates(m,state):
    await state.set_state(Form.text);await state.update_data(action='date')
    await calendar_view(m,state,now().year,now().month)

async def calendar_view(m,state,year,month):
    first=now().date().replace(day=1);last=(now().date()+timedelta(days=365)).replace(day=1)
    selected=datetime(year,month,1).date()
    if not first<=selected<=last:raise ValueError('Выберите месяц в пределах ближайшего года.')
    d=await state.get_data();s=await service_for(d)
    if not s:raise ValueError('Выберите услугу заново.')
    await state.set_state(Form.text);await state.update_data(action='date')
    names=['Январь','Февраль','Март','Апрель','Май','Июнь','Июль','Август','Сентябрь','Октябрь','Ноябрь','Декабрь']
    rows=[[(x,'v3:noop') for x in ['Пн','Вт','Ср','Чт','Пт','Сб','Вс']]]
    for week in calendar.monthcalendar(year,month):
        cells=[]
        for n in week:
            if not n:cells.append(('·','v3:noop'));continue
            day=datetime(year,month,n).date()
            if not now().date()<=day<=now().date()+timedelta(days=365):cells.append(('—','v3:noop'));continue
            status=await availability(s,day,d.get('move',0))
            label=str(n) if status=='free' else ('×' if status=='off' else '•')+str(n)
            cells.append((label,'v3:date:'+day.isoformat()))
        rows.append(cells)
    nav=[]
    prev=(selected-timedelta(days=1)).replace(day=1);nxt=(selected+timedelta(days=32)).replace(day=1)
    if prev>=first:nav.append(('←',f'v3:calendar:{prev.year}:{prev.month}'))
    if nxt<=last:nav.append(('→',f'v3:calendar:{nxt.year}:{nxt.month}'))
    if nav:rows.append(nav)
    rows += [[('Сегодня','day:0'),('Завтра','day:1')],[('Назад','v3:back'),('Отмена','home')]]
    await m.answer(f'{names[month-1]} {year}\n× — выходной; • — нет свободных окон.\nМожно ввести дату ДД.ММ.ГГГГ.',reply_markup=kb(rows))

async def back(m,uid,state):
    d=await state.get_data();a=d.get('action','')
    wizard=d.get('wizard')
    if wizard:
        stages={'profile':['name','bio','review'],'service':['name','minutes','price','review'],'contacts':['name','phone','review']}
        seq=stages[wizard];idx=seq.index(d['stage'])
        if idx:return await wizard_step(m,state,wizard,seq[idx-1])
        return await dispatch(m,uid,'services' if wizard=='service' else 'staff' if wizard=='profile' else 'home',state)
    if a in ('phone','confirm') and not d.get('move'):
        await state.set_state(Form.text);await state.update_data(action='name');return await m.answer('Введите имя клиента:',reply_markup=NAV)
    if a=='name' or (a=='confirm' and d.get('move')):return await dates(m,state)
    if a=='date' and d.get('mid') and not d.get('move'):return await select_services(m,state,d['mid'])
    if a=='clock' and d.get('which')=='end':return await clock(m,state,'start')
    if a in ('clock','step','target','block_confirm','schedule_date','block_date') and await master(uid):return await schedule_menu(m,state,uid)
    if uid==MANAGER:return await cabinet(m,uid,state,'manager')
    if await master(uid):return await cabinet(m,uid,state,'staff')
    return await home(m,uid,state)

async def wizard_step(m,state,kind,stage):
    await state.set_state(Form.text);await state.update_data(action='wizard',wizard=kind,stage=stage)
    d=await state.get_data();rows=[]
    if stage=='review':
        await state.set_state(None)
        if kind=='profile':text=f"Имя: {d['wname']}\nСпециализация: {d.get('wbio','')}"
        elif kind=='service':text=f"Услуга: {d['wname']}\nДлительность: {d['wminutes']} мин\nЦена: {d['wprice'] or 'без цены'}"
        else:text=f"Имя: {d['wname']}\nТелефон: {d['wphone']}"
        rows.append([('Сохранить','v3:wizard_save')])
    else:
        text={'name':'Введите имя:' if kind!='service' else 'Введите название услуги:','bio':'Укажите специализацию / описание:','minutes':'Введите длительность в минутах (5–720):','price':'Введите цену или выберите «Без цены»:','phone':'Введите телефон (10–15 цифр):'}[stage]
        if stage in ('bio','price'):rows.append([('Без описания' if stage=='bio' else 'Без цены','v3:skip')])
    rows.append([('Назад','v3:back'),('Отмена','home')])
    await m.answer(text,reply_markup=kb(rows))

async def wizard_value(m,uid,state,text):
    d=await state.get_data();kind=d['wizard'];stage=d['stage']
    if kind in ('profile','service') and not await master(uid):raise ValueError('Права мастера отозваны.')
    if kind=='contacts':
        b=await sql('SELECT * FROM bookings WHERE id=?',(d['bid'],),True)
        if not await access_booking(uid,b,True):raise ValueError('Нет прав.')
    if stage=='name':
        if not 1<=len(text)<= (100 if kind=='contacts' else 80):raise ValueError('Недопустимая длина имени/названия.')
        await state.update_data(wname=text)
        return await wizard_step(m,state,kind,{'profile':'bio','service':'minutes','contacts':'phone'}[kind])
    if stage=='bio':
        if len(text)>500:raise ValueError('Описание до 500 символов.')
        await state.update_data(wbio='' if text=='-' else text)
    elif stage=='minutes':
        if not text.isdecimal() or not 5<=int(text)<=720:raise ValueError('Длительность 5–720 минут.')
        await state.update_data(wminutes=int(text));return await wizard_step(m,state,kind,'price')
    elif stage=='price':
        price=None if text=='-' else text.replace(',','.')
        if price is not None and not re.fullmatch(r'\d{1,7}(\.\d{1,2})?',price):raise ValueError('Введите число или нажмите «Без цены».')
        await state.update_data(wprice=price)
    elif stage=='phone':
        phone=re.sub(r'[\s()\-]','',text)
        if not re.fullmatch(r'\+?\d{10,15}',phone):raise ValueError('Номер должен содержать 10–15 цифр.')
        await state.update_data(wphone=phone)
    else:raise ValueError('Используйте кнопки подтверждения.')
    await wizard_step(m,state,kind,'review')

async def wizard_save(m,uid,state):
    d=await state.get_data();kind=d.get('wizard')
    if d.get('stage')!='review':raise ValueError('Форма устарела.')
    if kind in ('profile','service') and not await master(uid):raise ValueError('Нет прав мастера.')
    if kind=='profile':
        await sql('UPDATE masters SET name=?,bio=? WHERE id=? AND enabled=1',(d['wname'],d['wbio'],uid))
        await m.answer('Профиль сохранён.');await cabinet(m,uid,state,'staff');return await readiness(m,uid)
    if kind=='service':
        if d.get('sid'):await sql('UPDATE services SET name=?,minutes=?,price=? WHERE id=? AND master=?',(d['wname'],d['wminutes'],d['wprice'],d['sid'],uid))
        else:await sql('INSERT INTO services(master,name,minutes,price) VALUES(?,?,?,?)',(uid,d['wname'],d['wminutes'],d['wprice']))
        await state.clear();await m.answer('Услуга сохранена. Существующие записи не изменены.');return await dispatch(m,uid,'services',state)
    if kind=='contacts':
        b=await sql('SELECT * FROM bookings WHERE id=?',(d['bid'],),True)
        if not await access_booking(uid,b,True):raise ValueError('Нет прав.')
        await sql('UPDATE bookings SET name=?,phone=? WHERE id=?',(d['wname'],d['wphone'],d['bid']))
        await state.clear();await m.answer('Контакты сохранены. Привязка Telegram не изменена.');return await card(m,uid,d['bid'])

async def schedule_menu(m,state,uid):
    await _schedule2(m,state,uid)
async def target_menu(m,state,uid,target):
    await _target2(m,state,uid,target)
async def clock(m,state,which):
    await _clock2(m,state,which)

async def danger(m,uid,k,state):
    d=await state.get_data()
    if not await master(uid):raise ValueError('Нет прав мастера.')
    text='Подтвердите действие. Существующие записи не отменяются.'
    # Snapshot the underlying timetable so a stale delete cannot target another interval.
    snapshot=None
    if k in ('off','reset') or k.startswith('interval_del:'):
        if d.get('action')!='target':raise ValueError('Откройте день заново.')
        r=await sql('SELECT intervals FROM v2_schedule WHERE master=? AND target=?',(uid,d['target']),True)
        snapshot=r['intervals'] if r else None
        ws=unpack(snapshot) if snapshot else []
        if k=='off':text='Сделать выбранный день выходным?'
        elif k=='reset':text='Удалить исключение и вернуть недельный график?'
        else:
            idx=int(k.split(':')[1])
            if not 0<=idx<len(ws):raise ValueError('Список изменился.')
            text=f'Удалить рабочий интервал {ws[idx][0]}–{ws[idx][1]}?'
        rows=await sql("SELECT * FROM bookings WHERE master=? AND status='active' AND visit_state='expected' AND start>?",(uid,ts()))
        affected=[]
        for b in rows:
            day=datetime.fromtimestamp(b['start'],TZ).date();target=d['target']
            if (target.startswith('w:') and day.weekday()==int(target[2:])) or target=='d:'+day.isoformat():affected.append(b)
        if affected:text+='\nПроверьте записи этого дня/дня недели: '+', '.join('#'+str(b['id']) for b in affected)[:2500]
    elif k=='block_save':
        if d.get('action')!='block_confirm':raise ValueError('Форма устарела.')
        day=datetime.strptime(d['day'],'%d.%m.%Y').date();a,b=bounds([d['start'],d['end']],day)
        rows=await sql("SELECT id,start FROM bookings WHERE master=? AND status='active' AND start<? AND end>?",(uid,b,a))
        text=f"Закрыть {d['day']} {d['start']}–{d['end']}?"
        if rows:text+='\nПересекаются записи: '+', '.join('#'+str(x['id'])+' '+fmt(x['start']) for x in rows)[:2500]
    elif k.startswith('hide:'):text='Скрыть услугу из каталога? Уже оформленные записи сохранятся.'
    elif k.startswith('unblock:'):text='Открыть закрытый интервал для новых записей?'
    nonce=secrets.token_hex(6)
    await state.update_data(danger=dict(key=k,nonce=nonce,snapshot=snapshot))
    await m.answer(text+'\nЗаписи клиентов автоматически НЕ отменяются.',reply_markup=kb([[('Подтвердить','v3:danger:'+nonce)],[('Назад','v3:back'),('Отмена','home')]]))

async def dispatch(m,uid,k,state):
    d=await state.get_data()
    if k=='v3:noop':return
    if k=='v3:back':return await back(m,uid,state)
    if k in ('staff','manager'):return await cabinet(m,uid,state,k)
    if k=='v3:ready':
        if await master(uid):return await readiness(m,uid)
        return
    if k=='v3:masters':
        if uid!=MANAGER:return
        await state.clear()
        rows=await sql('SELECT * FROM masters ORDER BY enabled DESC,name')
        if not rows:await m.answer('Мастеров пока нет.')
        for i in range(0,len(rows),25):await m.answer('Мастера:',reply_markup=kb([[((p['name'] or str(p['id']))+(' · отключён' if not p['enabled'] else ''),f"v3:master:{p['id']}")] for p in rows[i:i+25]]))
        return await m.answer('Действия:',reply_markup=kb([[('Выдать права','grant'),('В кабинет','manager')]]))
    if k.startswith('v3:master:'):
        if uid==MANAGER:return await master_card(m,int(k.split(':')[2]))
        return
    if k.startswith('v3:right:'):
        if uid!=MANAGER:return
        _,_,mid,value=k.split(':');mid=int(mid);value=int(value)
        if value not in (0,1):return
        p=await sql('SELECT * FROM masters WHERE id=?',(mid,),True)
        if not p:return
        count=await sql("SELECT COUNT(*) AS n FROM bookings WHERE master=? AND status='active' AND start>?",(mid,ts()),True)
        await state.set_data(dict(action='rights',mid=mid,value=value,old=p['enabled'],nonce=secrets.token_hex(6)))
        d=await state.get_data()
        return await m.answer(('Восстановить' if value else 'Отозвать')+f" права мастера {p['name'] or mid}?\nПредстоящих записей: {count['n']}. Они останутся у управляющего. Профиль, услуги и история сохранятся. Клиентские функции останутся доступны.",reply_markup=kb([[('Подтвердить','v3:rightok:'+d['nonce'])],[('Назад',f'v3:master:{mid}')]]))
    if k.startswith('v3:rightok:'):
        if uid!=MANAGER or d.get('action')!='rights' or k.split(':')[2]!=d.get('nonce'):raise ValueError('Подтверждение устарело.')
        await sql('UPDATE masters SET enabled=? WHERE id=? AND enabled=?',(d['value'],d['mid'],d['old']))
        target_context=dp.fsm.get_context(bot=bot,chat_id=d['mid'],user_id=d['mid'])
        await target_context.clear();await state.clear()
        await send(d['mid'],'Права мастера '+('восстановлены.' if d['value'] else 'отозваны. Клиентские функции сохранены.')+' Откройте главное меню.')
        return await master_card(m,d['mid'])
    if k.startswith('v3:manual:'):
        if uid!=MANAGER:return
        mid=int(k.split(':')[2]);await state.clear();await state.update_data(manual=True)
        return await select_services(m,state,mid)
    if k.startswith('v3:masterschedule:'):
        if uid!=MANAGER:return
        mid=int(k.split(':')[2]);lines=['Расписание мастера (просмотр), МСК:']
        for r in await sql('SELECT * FROM v2_schedule WHERE master=? ORDER BY target',(mid,)):
            label=['Пн','Вт','Ср','Чт','Пт','Сб','Вс'][int(r['target'][2:])] if r['target'].startswith('w:') else r['target'][2:]
            lines.append(label+': '+('; '.join(f'{a}–{b} / {step} мин' for a,b,step in unpack(r['intervals'])) or 'выходной'))
        for r in await sql('SELECT * FROM v2_blocks WHERE master=? AND end>? ORDER BY start',(mid,ts())):lines.append('Закрыто: '+fmt(r['start'])+' — '+fmt(r['end']))
        text='\n'.join(lines)
        for i in range(0,len(text),3500):await m.answer(text[i:i+3500])
        return await m.answer('Карточка мастера:',reply_markup=kb([[('Назад',f'v3:master:{mid}')]]))
    if k.startswith('v3:masterbook:'):
        if uid!=MANAGER:return
        p=k.split(':');return await history(m,uid,state,'master-'+p[2],int(p[3]))
    if k.startswith('v3:history:'):
        p=k.split(':');return await history(m,uid,state,p[2],int(p[3]))
    if k=='v3:search':
        if uid!=MANAGER and not await master(uid):return
        return await prompt(m,state,'search','Введите имя (регистр букв учитывается для кириллицы) или часть телефона:')
    if k.startswith(('v3:contacts:','v3:repeat:')):
        p=k.split(':');bid=int(p[2]);b=await sql('SELECT * FROM bookings WHERE id=?',(bid,),True)
        if not await access_booking(uid,b,True):raise ValueError('Нет доступа.')
        await state.clear()
        if p[1]=='contacts':
            await state.update_data(bid=bid);return await wizard_step(m,state,'contacts','name')
        if not await master(b['master']):raise ValueError('Права этого мастера отозваны.')
        s=await sql('SELECT * FROM services WHERE id=? AND active=1',(b['service'],),True)
        if not s:raise ValueError('Услуга скрыта. Создайте ручную запись с другой услугой.')
        await state.update_data(manual=True,mid=b['master'],sid=b['service'],name=b['name'],phone=b['phone'],repeat=bid)
        await m.answer('Повторная ручная запись: '+b['name']+'. Контакты сохранены; запись не привязывается к Telegram клиента.')
        return await dates(m,state)
    if k.startswith('v3:calendar:'):
        if d.get('action')!='date':raise ValueError('Откройте выбор даты заново.')
        p=k.split(':');return await calendar_view(m,state,int(p[2]),int(p[3]))
    if k.startswith('v3:date:'):
        if d.get('action')!='date':raise ValueError('Откройте выбор даты заново.')
        return await slots(m,state,datetime.strptime(k.split(':')[2],'%Y-%m-%d').date())
    if k.startswith('time:') and d.get('repeat'):
        _,sid,t=k.split(':')
        if d.get('action')!='date' or d.get('sid')!=int(sid):raise ValueError('Меню устарело.')
        await state.update_data(time=int(t));return await confirm(m,state)
    if k=='profile' or k=='addservice' or k.startswith('edit:'):
        if not await master(uid):return
        await state.clear()
        if k=='profile':return await wizard_step(m,state,'profile','name')
        sid=int(k.split(':')[1]) if ':' in k else 0
        if sid and not await sql('SELECT id FROM services WHERE id=? AND master=?',(sid,uid),True):return
        await state.update_data(sid=sid);return await wizard_step(m,state,'service','name')
    if k=='v3:skip':
        if d.get('wizard') and d.get('stage') in ('bio','price'):return await wizard_value(m,uid,state,'-')
        return
    if k=='v3:wizard_save':return await wizard_save(m,uid,state)
    if k.startswith('v3:danger:'):
        pending=d.get('danger',{})
        if pending.get('nonce')!=k.split(':')[2]:raise ValueError('Подтверждение устарело.')
        if not await master(uid):raise ValueError('Нет прав мастера.')
        key=pending['key']
        if key in ('off','reset') or key.startswith('interval_del:'):
            r=await sql('SELECT intervals FROM v2_schedule WHERE master=? AND target=?',(uid,d['target']),True)
            if (r['intervals'] if r else None)!=pending['snapshot']:raise ValueError('График изменился. Откройте день заново.')
        await state.update_data(danger=None)
        await _dispatch2(m,uid,key,state)
        if key.startswith('hide:'):return await _dispatch2(m,uid,'services',state)
        if key=='block_save' or key.startswith('unblock:'):return await schedule_menu(m,state,uid)
        return
    if k in ('off','reset','block_save') or k.startswith(('interval_del:','hide:','unblock:')):return await danger(m,uid,k,state)
    if k.startswith(('move:','cancel:','cancelok:','yes:','status:')):
        b=await sql('SELECT * FROM bookings WHERE id=?',(int(k.split(':')[1]),),True)
        staff=k.startswith('status:')
        if not await access_booking(uid,b,staff):raise ValueError('Нет доступа к записи.')
    await _dispatch2(m,uid,k,state)
    if k in ('my',) or k.startswith('list:'):
        await m.answer('Вернуться:',reply_markup=kb([[('Главное меню','home')]]))

@dp.message(F.text.in_({'Главное меню','Назад','Отмена'}))
async def navigation(m:Message,state:FSMContext):
    if m.text=='Назад':
        try:return await back(m,m.from_user.id,state)
        except (ValueError,KeyError):pass
    await home(m,m.from_user.id,state)

@dp.message(Form.text)
async def input_v3(m:Message,state:FSMContext):
    uid=m.from_user.id;text=(m.text or '').strip();d=await state.get_data()
    try:
        if d.get('action')=='wizard':
            if m.contact:text=m.contact.phone_number
            return await wizard_value(m,uid,state,text)
        if d.get('action')=='search':
            if uid!=MANAGER and not await master(uid):raise ValueError('Нет доступа.')
            if not 2<=len(text)<=100:raise ValueError('Введите от 2 до 100 символов.')
            await state.set_state(None);await state.update_data(search=text)
            return await history(m,uid,state,'search',0)
        if d.get('action')=='grant':
            if uid!=MANAGER:return
            if not text.isdecimal() or int(text)<=0:raise ValueError('Нужен числовой Telegram ID.')
            target=int(text)
            await sql('INSERT OR IGNORE INTO masters(id) VALUES(?)',(target,))
            p=await sql('SELECT * FROM masters WHERE id=?',(target,),True)
            if not p['enabled']:return await dispatch(m,uid,f'v3:right:{target}:1',state)
            await send(target,'Вам выданы права мастера. Откройте главное меню или /start.')
            await state.clear();await m.answer('Права выданы.');return await master_card(m,target)
        await _input2(m,state)
    except (ValueError,OverflowError,KeyError) as e:
        await m.answer('Проверьте ввод: '+str(e),reply_markup=NAV)

@dp.message()
async def fallback_v3(m:Message,state:FSMContext):
    await m.answer('Выберите действие в меню. Ваш ID можно узнать командой /id.',reply_markup=NAV)
    await home(m,m.from_user.id,state)

if __name__=='__main__':asyncio.run(main())

