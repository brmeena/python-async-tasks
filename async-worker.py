import sys
import traceback
import asyncio
import aiohttp
import aiofiles
import motor.motor_asyncio
mconn=motor.motor_asyncio.AsyncIOMotorClient("mongodb://localhost:27017")
db=mconn['async_worker_demo_db']
collection=db['async_worker_demo_coll']
async def read_file(file_path):
    f=await aiofiles.open(file_path, mode='r')
    content=await f.read()
    await f.close()
    return content
async def fetch_url(url):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            return await response.text()

async def mongo_read():
    mongo_objs=collection.find({})
    return await mongo_objs.to_list(length=None)

async def mongo_save():
    await collection.insert_one({"name":"test"})

async def worker(worker_name, queue):
    print("in worker with name ",worker_name)
    while True:
        task_data=await queue.get()
        if task_data is None:
            break
        try:
            if task_data['task_purpose']=='file_reader':
                content=await read_file(task_data['file_path'])
                print(f'File content: {content}')
            elif task_data['task_purpose']=='url_fetcher':
                content=await fetch_url(task_data['url'])
                print(f'URL content: {content}')
            elif task_data['task_purpose']=='mongo_reader':
                data=await mongo_read()
                print(f'Mongo data: {data}')
            elif task_data['task_purpose']=='mongo_saver':
                await mongo_save()
                print(f'Mongo data saved')
        except Exception as e:
            print(f'Error in worker {worker_name}: {e}')
            traceback.print_exc()
        queue.task_done()
async def start_process():
    queue = asyncio.Queue()
    tasks = []
    for i in range(50):
        task = asyncio.create_task(worker(f"worker-{i}", queue))
        tasks.append(task)
    task_data = {
        'task_purpose': 'file_reader',
        "file_path": "data/input.txt",
    }
    queue.put_nowait(task_data)
    task_data = {
        'task_purpose': 'url_fetcher',
        "url": "https://www.google.com",
    }
    queue.put_nowait(task_data)
    task_data = {
        'task_purpose': 'mongo_reader',
    }
    queue.put_nowait(task_data)
    task_data = {
        'task_purpose': 'mongo_saver',
    }
    queue.put_nowait(task_data)
    await queue.join()
    # Wait until all worker tasks are cancelled.
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


asyncio.run(start_process())