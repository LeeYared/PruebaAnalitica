from fastapi import FastAPI, UploadFile, File, HTTPException
import pandas as pd
from core.archivo import  create_table_from_csv


app = FastAPI()

@app.post("/upload_csv/")
async def upload_csv(file: UploadFile = File(...)):
  try:
    df = pd.read_csv(file.file)
    table_name = file.filename.split(".")[0]  # Nombre de la tabla basado en el archivo
    table = create_table_from_csv(df, table_name)
    return{"message": f"Datos insertados correctamente en la tabla {table_name}"}
  except Exception as e:
    raise HTTPException(status_code=500, detail=str(e))


