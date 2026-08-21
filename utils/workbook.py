import asyncio
from openpyxl import Workbook, load_workbook
from openpyxl.styles import PatternFill, Font, Alignment
from io import BytesIO
from zipfile import is_zipfile

def snake_to_text(text):
    return text.replace('_', ' ').replace('-', ' ').capitalize()

def create(attributes: list = None, fields: list| None = None, names: list| None = None):
    """
    CREATES THE XLSX FILE FOR THE PRODUCT IF attributes={list(dict)} is provided\n
    ARGUMENT:\n
    \t  attributes:\n
    \t \t   List [] of dicts of attributes containing field, name, description, required\n
    ELSE:\n
    an empty sheet will be created
    """
    book = Workbook()
    sheet = book.active
    sheet.title = "Product bulk upload"
    try:
        if not attributes:
            if not (fields and names):
                return {"error": "fields or names row not provided"}

            sheet.append(fields)
            sheet.append(names) 

        else:
            '''CREATE SHEET WITH A DESCRIPTION'''
            rows = ["field", "name", "description"]
            for i, field in enumerate(rows): # because we wanna write hidden field, name and description
                for j, attribute in enumerate(attributes):
                    sheet.cell(row=i+1, column=j+1).value = attribute.get(field) + " *" if (field == "name" and attribute.get("required") == True) else attribute.get(field)

            sheet.cell(row=len(rows)+1, column=1).value = "DELETE THE DESCRIPTION ROW AND THIS MESSAGE BEFORE UPLOADING AND START FROM ROW 3 TO FILL THE DATA"


        font = Font(name="Calibri", size=13, bold=True)
        align = Alignment(horizontal='center', vertical='center')
        for cell in sheet[2]:
            cell.font = font
            cell.alignment = align
            # cell.fill = fill

        sheet.row_dimensions[1].hidden = True
        stream = BytesIO()
        book.save(stream)
        stream.seek(0)
        return stream
    except Exception as e:
        print(f"error encountered while creating workbook for product", e)
    finally:
        book.close()

    

def read_generator(file: Workbook):
    """Read the rows and yield them from generator
    
    Paramters:
        file: xlsx file

    Returns:
        list of product attributes with field `name`, `field` and `value`
        ``[
            {
                "name": str,
                "field": str,
                "value": str| int| None
            }
        ]``
    """
    file.seek(0)
    wb = load_workbook(file)
    ws = wb.active
    
    rows = ws.iter_rows(values_only=True)
    fields = next(rows)
    names = next(rows) # it's not getting in use

    
    for row in rows:
        yield [{"name": names[index], "field": fields[index], "value": value} for index, value in enumerate(row)]


def read_row(file: Workbook, row: int)-> list[str]:
    """Reads single row from the Workbook and resets the pointer back to 0
    Returns:
        list of cell values of the given row
    """
    # print("read_row", file)
    file.seek(0)

    # print("closed:", file.closed)
    # print("size:", len(file.getbuffer()))
    # print("is_zip:", is_zipfile(file))
    # print(file.read())
    wb = load_workbook(file, read_only=True)
    ws = wb.active

    cells = ws[row]
    file.seek(0)
    return [cell.value for cell in cells if cells]



def write(file: Workbook, row_object: dict = None, row: list| None = None):
    file.seek(0)
    wb = load_workbook(file)
    ws = wb.active

    '''we have to consider that the worksheet that has been uploaded is the correct worksheet for the typeid
        and contains the necessary headers
    '''
    try:

        if row:
            ws.append(row)
        else:
            '''match the header with the dict and make a new list of values mathcing the list of headers'''
            row = list(row_object.values())
            ws.append(row)

        buffer = BytesIO()
        wb.save(buffer)
        buffer.seek(0)
    except:
        return "error"
    return buffer


def remove_row(file: Workbook, index: int):
    file.seek(0)
    wb = load_workbook(file)
    ws = wb.active

    ws.delete_rows(index)
    buffer = BytesIO()
    wb.save(buffer)
    buffer.seek(0)
    return buffer




if __name__ == "__main__":
    asyncio.to_thread(create(["col1", "last_visit"]))
