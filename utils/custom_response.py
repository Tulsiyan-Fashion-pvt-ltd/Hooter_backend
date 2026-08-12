from quart import Response
from io import BytesIO
import json
import uuid


def make_multipart_response(
    json_data,
    xlsx_data,
    filename="failed_rows.xlsx"
):
    boundary = f"----Boundary{uuid.uuid4().hex}"

    if isinstance(xlsx_data, BytesIO):
        xlsx_data.seek(0)
        xlsx_data = xlsx_data.read()

    body = bytearray()

    body.extend(
        (
            f"--{boundary}\r\n"
            "Content-Type: application/json; charset=utf-8\r\n"
            "\r\n"
        ).encode()
    )

    body.extend(json.dumps(json_data).encode())

    body.extend(
        (
            f"\r\n--{boundary}\r\n"
            "Content-Type: application/vnd.openxmlformats-officedocument.spreadsheetml.sheet\r\n"
            f'Content-Disposition: attachment; filename="{filename}"\r\n'
            "\r\n"
        ).encode()
    )

    body.extend(xlsx_data)

    body.extend(
        f"\r\n--{boundary}--\r\n".encode()
    )

    return Response(
        bytes(body),
        status=202,
        content_type=f"multipart/mixed; boundary={boundary}"
    )