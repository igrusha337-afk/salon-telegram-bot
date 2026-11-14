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
    await m.answer(text+'\n/start — выйти в меню.')
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
    p=await master(s['master'])
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
        if d.get('manual') and uid not in (s['master'],MANAGER):raise ValueError('Нет прав.')
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

@dp.message(Form.text)
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

@dp.message()
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
if __name__=='__main__':asyncio.run(main())
