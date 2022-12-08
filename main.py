from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from apscheduler.schedulers.background import BackgroundScheduler
import pymysql
from datetime import datetime, timedelta

app = FastAPI()
origins = [
    "*"
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
conn = pymysql.connect(host='earlivery.cckpfejc2svw.ap-northeast-2.rds.amazonaws.com',
                       user='admin',
                       password='earlivery0102!',
                       db='earlivery',
                       charset='utf8')
sched = BackgroundScheduler()


class CallSched(BaseModel):
    start_time: str
    writing_cycle: int
    account: str
    uid: int


def job(shed_id, uid):
    conn = pymysql.connect(host='earlivery.cckpfejc2svw.ap-northeast-2.rds.amazonaws.com',
                           user='admin',
                           password='earlivery0102!',
                           db='earlivery',
                           charset='utf8')
    current_date = datetime.strptime(datetime.today().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")
    with conn:
        with conn.cursor() as cur:
            cur.execute("insert into summary(user_id) values (%s)", uid)
            cur.execute(
                "select e.device_number, i.name, i.category, i.code, drd.weight,  drd.battery, l.branch_name, l.warehouse_name, l.layer_name, drd.created_at, drd.data_interval, c.weight as container_weight from (select earlivery_device_id, max(created_at) as max_date from device_raw_data group by earlivery_device_id) as t2, device_raw_data drd left join earlivery_device e on drd.earlivery_device_id = e.id left join item i on i.id = e.item_id left join location l on e.location_id = l.id left join container c on c.id = e.container_id left join user u on l.user_id = u.id where drd.earlivery_device_id = t2.earlivery_device_id and drd.created_at = t2.max_date and u.id = %s",
                uid)
            report_form = cur.fetchall()
            if len(report_form) < 1:
                return 0
            conn.commit()
            cur.execute("select last_insert_id() as last")
            summary_id = cur.fetchall()[0][0]
            for data in report_form:
                device_number = data[0]
                item_name = data[1]
                category = data[2]
                code = data[3]
                weight = data[4]
                battery = data[5]
                branch_name = data[6]
                warehouse_name = data[7]
                layer_name = data[8]
                last_date_time = data[9]
                data_interval = data[10]
                container_weight = data[11]
                connect_status = 'normal'
                cur.execute(
                    "select device_number, drd.data_interval, drd.created_at from (select earlivery_device_id, max(created_at) as max_date from device_raw_data group by earlivery_device_id) as t2, earlivery_device left join item i on earlivery_device.item_id = i.id left join device_raw_data drd on earlivery_device.id = drd.earlivery_device_id where earlivery_device.device_number = %s and t2.max_date = drd.created_at",
                    device_number)
                connect_check = cur.fetchone()
                # 연결상태 체크
                compare_date = current_date - timedelta(minutes=int(data_interval) + 5)
                if connect_check[2] < compare_date:
                    connect_status = 'warning'
                cur.execute(
                    "select sum(drd.weight) from (select earlivery_device_id, max(created_at) as max_date from device_raw_data group by earlivery_device_id) as t2, earlivery_device left join item i on earlivery_device.item_id = i.id left join device_raw_data drd on earlivery_device.id = drd.earlivery_device_id where i.code = %s and t2.max_date = drd.created_at",
                    code)
                total_stock = cur.fetchall()
                cur.execute(
                    "select sum(drd.weight) from (select earlivery_device_id, max(created_at) as max_date from device_raw_data where device_raw_data.created_at not in (select max(created_at) from device_raw_data group by earlivery_device_id) group by earlivery_device_id) as t2, earlivery_device left join item i on earlivery_device.item_id = i.id left join device_raw_data drd on earlivery_device.id = drd.earlivery_device_id where earlivery_device.device_number = %s and t2.max_date = drd.created_at",
                    device_number)
                pretotal_stock = cur.fetchall()
                usage_weight = total_stock[0][0] - pretotal_stock[0][0]
                if usage_weight < 0:
                    usage_weight = 0
                val = (device_number, shed_id, category, code, weight, battery, branch_name, warehouse_name, layer_name,
                       connect_status, last_date_time, data_interval, usage_weight, container_weight, summary_id)
                cur.execute(
                    "insert into summary_content (device_number, item_name, item_category, item_code, weight, battery, branch_name, warehouse_name, layer_name, connection, last_date_time, `interval`, usage_weight, container_weight, summary_id) values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    val)
                conn.commit()
                a = 1
            print(connect_check)


def scheduling_job(interval, start_time, id, uid):
    sched.add_job(lambda: job(id, uid), 'interval', hours=interval, id=id)
    sched.start()


@app.get("/")
async def root():
    return {"message": "tripod-scheduler"}


@app.get("/sched/")
async def scheduler(callSched: CallSched):
    scheduling_job(callSched.writing_cycle, callSched.start_time, callSched.account, callSched.uid)
    return callSched


@app.get("/sched_change/")
async def modify(callSched: CallSched):
    sched.remove_job(callSched.id)
    scheduling_job(callSched.writing_cycle, callSched.start_time, callSched.account, callSched.uid)
    return callSched
