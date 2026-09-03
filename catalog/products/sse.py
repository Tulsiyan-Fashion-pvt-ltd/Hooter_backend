import asyncio
import json
from quart import Blueprint,url_for, request, Response, jsonify  # Quart has built-in SSE support
from . import services

product_sse = Blueprint("product_sse", __name__, url_prefix = "/events")


# 1. SSE Endpoint that the client connects to
@product_sse.get("/<job_id>")
async def xlsx_upload_stream(job_id):
    """SSE generator which sustains the http connection with the api """
    if "text/event-stream" not in request.headers.get("Accept", ""):
        return jsonify({"status": "failed", "message": "Expected event stream"}), 406

    if job_id not in services.tasks:
        return jsonify({"status": "failed", "message": "Job is not available with this task"}), 422 

    error_url = url_for(
        "catalog.products.send_error_sheet",
        job_id=job_id
    )

    async def event_generator():     
        if services.tasks.get(job_id).get("status") == "pending":
            yield f"data: {json.dumps({'status': 'pending', "message": "Product is uploading"})}\n\n"
            await services.tasks[job_id]["event"].wait()

        if services.tasks.get(job_id).get("status") == "failed":
            yield f"data: {json.dumps({'status': 'finsihed', 'message': 'Product upload failed'})}\n\n"
        else:
            task = services.tasks[job_id].get("task")
            returned_value_of_submit_task = task.result()

            if returned_value_of_submit_task.get("code") == 200:
                yield f"data: {json.dumps({
                                    'message': returned_value_of_submit_task.get('message'),
                                    'status': returned_value_of_submit_task.get('status')
                                })}\n\n"

                '''pop the jobid from the tasks if the status code is ok'''
                services.tasks.pop(job_id)
            elif returned_value_of_submit_task.get("code") == 202:
                yield f"data: {json.dumps({
                                    'download_link': error_url,
                                    "status": "finished",
                                    "message": "Some products could not be uploaded"
                                })}\n\n"
                '''do not pop the data cuz the error sheet needs to be downloaded and 
                pop the job id from the tasks from the services there'''           


    return Response(
        event_generator(),
        content_type="text/event-stream"
    )