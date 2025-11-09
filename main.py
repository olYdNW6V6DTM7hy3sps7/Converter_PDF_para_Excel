"""
PDF ➜ Excel Converter API
- Framework: FastAPI (async, validation, OpenAPI)
- PDF parsing: pdfplumber
- DataFrames: pandas
- Excel writing: openpyxl (via pandas)
- Processing: fully in-memory (io.BytesIO)
- Endpoint: POST /api/v1/convert/pdf-to-excel

Behavior:
- Accepts a PDF via multipart/form-data (field "pdf_file")
- Extracts all tables on all pages (each table -> separate sheet)
- Returns a single .xlsx file as an attachment
- Robust error handling with precise HTTP status codes
"""

from __future__ import annotations

import io
import logging
import re
import os # Importação para ler variáveis de ambiente
from typing import Generator, Iterable, List, Tuple, Dict

import pandas as pd
import pdfplumber
from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.responses import StreamingResponse
from starlette import status
from fastapi.middleware.cors import CORSMiddleware # Importação para o CORS

# ------------------------------------------------------------------------------
# Logging
# ------------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("pdf2xlsx")

# ------------------------------------------------------------------------------
# Constants
# ------------------------------------------------------------------------------
EXCEL_MEDIA_TYPE = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
ATTACHMENT_FILENAME = "converted_output.xlsx"
INVALID_SHEET_CHARS = r'[:\\/*?\[\]]'

# Cabeçalho limpo e FIXO.
MANUAL_CLEAN_HEADERS = ["Turma", "Aluno", "Telefone", "Responsável"]
NUM_DATA_COLUMNS = len(MANUAL_CLEAN_HEADERS) - 1 # 3 colunas de dados

# Regex projetada para capturar os 3 grupos de dados do seu PDF
# Padrão: "Aluno","Telefone","Responsável"
# Captura o texto entre aspas, permitindo caracteres especiais nos grupos
# A regex é complexa para lidar com as aspas duplas, quebras de linha e vírgulas.
DATA_ROW_REGEX = re.compile(r'"\s*([^"]+?)\s*"\s*,\s*"(\(31\)[^"]+?|\s*Não\s*informou\s*)"\s*,\s*"([^"]+?)"')


# ------------------------------------------------------------------------------
# Configuration from Environment
# ------------------------------------------------------------------------------
# A variável de ambiente que armazenará os sites permitidos, separados por vírgula.
ALLOWED_ORIGINS_STRING = os.getenv("ALLOWED_ORIGINS")

if ALLOWED_ORIGINS_STRING:
    origins = [o.strip() for o in ALLOWED_ORIGINS_STRING.split(',') if o.strip()]
else:
    origins = []

if not origins:
    logger.warning("A variável ALLOWED_ORIGINS não está definida ou está vazia. A API bloqueará todas as requisições cross-origin.")
else:
    logger.info(f"CORS configurado para as origens: {', '.join(origins)}")


# ------------------------------------------------------------------------------
# FastAPI app
# ------------------------------------------------------------------------------
app = FastAPI(
    title="PDF to Excel Converter API",
    version="1.0.0",
    description=(
        "Uploads a PDF, extracts all detected tables using pdfplumber, writes each table "
        "to a separate Excel sheet, and returns the .xlsx file."
    ),
    contact={"name": "Backend Team"},
    license_info={"name": "MIT"},
)

# ------------------------------------------------------------------------------
# Middleware para Gerenciamento de CORS
# ------------------------------------------------------------------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,       
    allow_credentials=True,      
    allow_methods=["*"],         
    allow_headers=["*"],         
)


# ------------------------------------------------------------------------------
# Utility functions
# ------------------------------------------------------------------------------
def is_probably_pdf(file_bytes: bytes) -> bool:
    """
    Validate content by checking the PDF magic header.
    """
    return file_bytes.startswith(b"%PDF-")


def sanitize_sheet_name(name: str, used: set[str]) -> str:
    """
    Make a string a valid Excel sheet name and ensure uniqueness (<= 31 chars and no invalid chars).
    """
    sanitized = re.sub(INVALID_SHEET_CHARS, " ", name).strip() or "Sheet"
    if len(sanitized) > 31:
        sanitized = sanitized[:31]
    base = sanitized
    counter = 2
    while sanitized in used:
        suffix = f"_{counter}"
        # Ensure length <= 31 including suffix
        if len(base) + len(suffix) > 31:
            sanitized = base[: 31 - len(suffix)] + suffix
        else:
            sanitized = base + suffix
        counter += 1
    used.add(sanitized)
    return sanitized


def iter_bytesio(buf: io.BytesIO, chunk_size: int = 1024 * 1024) -> Iterable[bytes]:
    """
    Stream a BytesIO in chunks to avoid duplicating memory.
    """
    buf.seek(0)
    while True:
        chunk = buf.read(chunk_size)
        if not chunk:
            break
        yield chunk


def _pad_or_truncate(row: List[object] | None, size: int) -> List[object]:
    """
    Ensure each row has exactly 'size' columns.
    """
    r = list(row) if row is not None else []
    if len(r) < size:
        r.extend([None] * (size - len(r)))
    elif len(r) > size:
        r = r[:size]
    return r

def extract_tables_from_pdf(pdf_bytes: bytes) -> List[Tuple[pd.DataFrame, str]]:
    """
    Extrai dados do PDF usando a extração de texto e Regex para forçar a estrutura.
    """
    results: List[Tuple[pd.DataFrame, str]] = []
    
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        if not pdf.pages:
            return results

        for page_index, page in enumerate(pdf.pages, start=1):
            
            # 1. Capturar o nome da Turma (do topo da página)
            class_name = ""
            sheet_name_base = f"Page_{page_index}"
            
            try:
                # Extrai o texto da área superior (Y=0 a Y=70) para a Turma
                top_area = page.crop((0, 0, page.width, 70)) 
                top_text = top_area.extract_text().strip()
                
                if top_text:
                    match_turma = re.search(r"^\dº\s+(.*)", top_text)
                    if match_turma:
                        # Pega o primeiro segmento do nome da turma (ex: "Segurança" de "3º Segurança")
                        class_name = match_turma.group(1).strip().split('\n')[0].strip()
                        sheet_name_base = class_name
            except Exception:
                pass
            
            # 2. Extrair o texto da área de dados (excluindo a área superior e o rodapé)
            # Y=70 é logo abaixo do cabeçalho da tabela ruidoso.
            # Y=page.height - 20 (margem do rodapé)
            try:
                data_area = page.crop((0, 70, page.width, page.height)) 
                page_text = data_area.extract_text(layout=True)
            except Exception:
                page_text = ""
                
            if not page_text:
                continue
                
            # 3. Limpeza do texto bruto para facilitar a Regex
            # Remove as quebras de linha que não estão entre aspas duplas, pois o PDF tem quebras de linha dentro das células.
            # Este é o ponto mais crítico e a razão dos erros anteriores.
            clean_text = re.sub(r'(?<!")\s*\n\s*(?!")', ' ', page_text)
            
            # 4. Extração de dados usando a Regex Agressiva
            # Encontra todos os registros de dados que correspondem ao padrão: "Aluno","Telefone","Responsável"
            data_matches = DATA_ROW_REGEX.findall(clean_text)
            
            data_rows = []
            if data_matches:
                for match in data_matches:
                    # O match já retorna 3 grupos (Aluno, Telefone, Responsável)
                    cleaned_row = [item.strip() for item in match]
                    data_rows.append(cleaned_row)

            
            if not data_rows:
                logger.warning(f"Regex não encontrou dados para Turma: {class_name if class_name else f'Página {page_index}'}. A Turma provavelmente está vazia.")
                continue

            # 5. Criar o DataFrame
            df = pd.DataFrame(data_rows, columns=MANUAL_CLEAN_HEADERS[1:])

            # Adiciona a coluna 'Turma' (Class Name) como a primeira coluna
            df.insert(0, "Turma", class_name if class_name else f"Página {page_index}")
            
            # Limpeza final de valores vazios (NaN)
            df.replace(r"^\s*$", pd.NA, regex=True, inplace=True)
            df.dropna(subset=MANUAL_CLEAN_HEADERS[1:], how="all", inplace=True)

            results.append((df, sheet_name_base))

    return results

# ------------------------------------------------------------------------------
# Routes (Restante do código da API permanece o mesmo)
# ------------------------------------------------------------------------------
@app.post(
    "/api/v1/convert/pdf-to-excel",
    summary="Convert a PDF file containing tables to a single Excel workbook",
    responses={
        200: {"content": {EXCEL_MEDIA_TYPE: {}}},
        400: {
            "description": "Invalid file type or missing file",
            "content": {"application/json": {"example": {"detail": "Invalid file type. Please upload a PDF."}}},
        },
        422: {
            "description": "No tables detected in the provided PDF",
            "content": {"application/json": {"example": {"detail": "No tables could be found in the provided PDF."}}},
        },
        500: {
            "description": "Unexpected server error during processing",
            "content": {"application/json": {"example": {"detail": "An internal error occurred during file processing."}}},
        },
    },
    tags=["conversion"],
)
async def convert_pdf_to_excel(pdf_file: UploadFile = File(..., description="The PDF file to convert")) -> StreamingResponse:
    """
    Synchronous conversion endpoint:
    - Accepts a PDF via multipart/form-data (field: pdf_file)
    - Extracts tables and writes each table to a separate Excel sheet
    - Streams the .xlsx file back to the client
    """
    if pdf_file is None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid file type. Please upload a PDF.")

    try:
        file_bytes = await pdf_file.read()
    except Exception:
        logger.exception("Failed to read uploaded file stream.")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail="An internal error occurred during file processing.")

    if not file_bytes or not is_probably_pdf(file_bytes):
        # Strict content-sniffing check for PDFs
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid file type. Please upload a PDF.")

    # Extract tables -> DataFrames
    try:
        extracted = extract_tables_from_pdf(file_bytes)
    except HTTPException:
        # Re-levanta a exceção se ela já foi definida (ex: Configuration Error 500)
        raise
    except Exception:
        logger.exception("Unhandled error during PDF parsing and table extraction.")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail="An internal error occurred during file processing.")

    if not extracted:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail="No tables could be found in the provided PDF.")

    # Write DataFrames to an in-memory Excel workbook
    output_buf = io.BytesIO()
    try:
        used_sheet_names: set[str] = set()
        with pd.ExcelWriter(output_buf, engine="openpyxl") as writer:
            # Concatena todos os DataFrames em um único DF para a sheet "Geral"
            all_dfs = [df for df, _ in extracted]
            
            if all_dfs:
                df_general = pd.concat(all_dfs, ignore_index=True)
                
                # Garante a ordem correta das colunas
                cols = MANUAL_CLEAN_HEADERS
                df_general = df_general[cols]

                safe_name = sanitize_sheet_name("Geral - Contatos", used_sheet_names)
                df_general.to_excel(writer, index=False, sheet_name=safe_name)
            
        output_buf.seek(0)
    except Exception:
        logger.exception("Failed creating the Excel workbook.")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail="An internal error ocorreu durante file processing.")

    headers = {"Content-Disposition": f'attachment; filename="{ATTACHMENT_FILENAME}"'}
    return StreamingResponse(iter_bytesio(output_buf), media_type=EXCEL_MEDIA_TYPE, headers=headers)


@app.get("/health", tags=["health"])
def health() -> dict[str, str]:
    return {"status": "ok"}


if __name__ == "__main__":
    # For local development only. In containers, use the Dockerfile CMD.
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
