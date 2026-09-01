from quart import Blueprint, jsonify, request, Response
from utils.prerequirements import login_required, brand_required
from . import services

categories = Blueprint("categories", __name__, url_prefix = "/categories")


@categories.get("/top") 
@login_required
@brand_required
async def list_top_level_categories():
    categories = await services.top_level_attributes()
    return jsonify({"level0": categories})


@categories.get("/next/<path:id>")
@login_required
@brand_required
async def sub_categories(id):
    vertical = request.args.get("vertical", type=int)

    """CHECKING THE REQUIREMENTS"""
    if None in (vertical, id):
        return jsonify({"status": "failed", "message": "vertical(index) and category id is not provided"}), 400

    attributes = await services.next_level_attributes(id, vertical)
    
    return jsonify({"next": attributes}), 200



@categories.get('/attributes/<path:id>')
@login_required
@brand_required
async def get_attribute_fields(id):
    """
    RETURNS THE PRODUCT ATTRIBUTE
    some data are category specific soo for the front end to show them, it has to fetch it first
    this route will provide the data fields which for category specific attributes
    """
    category_id = id
    vertical = request.args.get("vertical", type=int)
    # print(category_id)
    #sanitising the arguments
    if category_id is None:
        return jsonify({'status': "invalid argument", "message": "no niche field available, it should be ?type-id=<id>"}), 400

    result = await services.get_product_attributes(category_id, vertical)
    return jsonify(result[0]), result[1]



# get the xlsx sheet for bulk upload
@categories.get('/bulk-excel-sheet/<path:id>')
@login_required
@brand_required
async def get_bulk_upload_sheet(id):
    """
    DOWNLOAD FUNCTION FOR THE XLSX EXCEL SHEET
    """
    vertical = request.args.get("vertical", type=int)

    bulk_sheet = await services.get_product_bulkupload_workbook(id, vertical)
    print(bulk_sheet)
    if type(bulk_sheet) == tuple:
        return jsonify(bulk_sheet[0]), bulk_sheet[1]

    return  Response(bulk_sheet, 
                     mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                     headers={
                        "Content-Disposition": f"attachment; filename=categories_{id}.xlsx"
                })