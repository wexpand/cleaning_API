from fastapi import FastAPI, UploadFile, File, HTTPException
from pydantic import BaseModel
from typing import List, Dict
from services.importers.apollo_importer import import_apollo_csv
from services.importers.apify_importer import import_apify_csv
from services.importers.contactout_importer import import_contact_csv
from services.importers.recruitcrm_importer import import_recruitcrm_csv
from services.candidate_query import search_candidates_by_skills
from services.update_handler import update_candidate

app = FastAPI()

class SkillsRequest(BaseModel):
    skills: List[str]

@app.post("/import/apollo")
async def import_apollo(file: UploadFile = File(...)):
    try:
        contents = await file.read()
        rows = import_apollo_csv(contents)
        return {"status": "ok", "rows_inserted": rows}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@app.post("/import/apify")
async def import_apify(file: UploadFile = File(...)):
    try:
        contents = await file.read()
        rows = import_apify_csv(contents)
        return {"status": "ok", "rows_inserted": rows}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/import/contactout")
async def import_contactout(file: UploadFile = File(...)):
    try:
        contents = await file.read()
        rows = import_contact_csv(contents)
        return {"status": "ok", "rows_inserted": rows}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@app.post("/import/recruitcrm")
async def import_recruitcrm(file: UploadFile = File(...)):
    try:
        contents = await file.read()
        rows = import_recruitcrm_csv(contents)
        return {"status": "ok", "rows_inserted": rows}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))    

@app.post("/candidates/search")
def search_candidates(req: SkillsRequest):
    try:
        results = search_candidates_by_skills([skill.lower() for skill in req.skills])
        return {"results": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/candidates/update")
def update_candidate_data(data: Dict):
    try:
        result = update_candidate(data)
        return {"status": "ok", "message": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/")
def root():
    return {"message": "Recruitment API running"}
