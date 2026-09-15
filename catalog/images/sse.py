tasks = {}
'''tasks stores the `job_id` as key and dict as values with keys
`task` -> Containing the task object from create_task
`event` -> asyncio.Event()
`progress` -> current background task progress in percentage
'''

from quart import Blueprint, jsonify, Response, request
from utils.prerequirements import login_required, brand_required
import json

image_sse = Blueprint('image_sse', __name__, url_prefix="/events")


@image_sse.get("/<job_id>")
@login_required
@brand_required
async def images_upload_sse(job_id):
    if "text/event-stream" not in request.headers.get("Accept", ""):
        return jsonify({"status": "failed", "message": "Expected event stream"}), 406
    
    if job_id not in tasks:
        return jsonify({'status': 'failed', 'message': 'job_id does not exists'}), 400

    async def event_stream():
        event = tasks.get(job_id).get('event')
        task =  tasks.get(job_id).get('task')
        result = None
        while True:
            event.clear()
            if task.done():
                break

            yield (f"event: progress\n"
                   f"data: {json.dumps({'status': 'pending', 
                                        'message': 'Images are getting uploaded', 
                                        'progress': tasks.get(job_id).get('progress')})}\n\n")
            await event.wait()

        if task.exception():
            print(task.exception())
            yield   (f"event: completed\n"
                    f"data: {json.dumps({'status': 'failed', 
                                        'message': 'Unexpected error occurred', 
                                        'progress': tasks.get(job_id).get('progress')})}\n\n")
        else:
            yield (f"event: completed\n"
                   f"data: {json.dumps({"result": task.result(),
                                        'progress': tasks.get(job_id).get('progress')})}\n\n")

        tasks.pop(job_id)

    return Response(
        event_stream(),
        content_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
        },
    )
