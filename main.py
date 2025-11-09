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
from typing import Generator, Iterable, List, Tuple

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

# ATENÇÃO: COORDENADAS FIXAS PARA EXTRAÇÃO ROBUSTA
# Estas coordenadas (pontos X) definem onde as colunas devem ser cortadas.
# Elas são a forma mais garantida de capturar colunas desalinhadas como 'Turma'.
# Valores de exemplo para um documento A4 típico (0 a 600 pontos):
# Exemplo: [50, 150, 250, 350, 450, 550] - Seis colunas.
# Por favor, ajuste se o PDF tiver margens ou layouts diferentes.
FIXED_COLUMN_COORDINATES = [
    50,  # Coluna 1 (Início da primeira coluna)
    130, # Divisão Coluna 1 / Coluna 2
    210, # Divisão Coluna 2 / Coluna 3
    300, # Divisão Coluna 3 / Coluna 4 (Onde estaria a 'Turma'?)
    420, # Divisão Coluna 4 / Coluna 5
    500, # Divisão Coluna 5 / Coluna 6
    550  # Fim da última coluna
]

# ------------------------------------------------------------------------------
# Configuration from Environment
# ------------------------------------------------------------------------------
# A variável de ambiente que armazenará os sites permitidos, separados por vírgula.
# Exemplo: "https://site1.com,https://site2.org,http://localhost:3000"
ALLOWED_ORIGINS_STRING = os.getenv("ALLOWED_ORIGINS")

# Configura a lista de origens. Se a variável estiver definida, ela é transformada
# em uma lista de strings. Caso contrário, a lista fica vazia, bloqueando o acesso
# de qualquer origem cross-origin.
if ALLOWED_ORIGINS_STRING:
    # Divide a string por vírgulas e remove espaços em branco (strip)
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
# Este middleware verifica o cabeçalho 'Origin' da requisição.
# Se a origem não estiver na lista 'origins', a requisição será bloqueada pelo navegador.
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,       # A lista de origens permitidas (lida da variável de ambiente)
    allow_credentials=True,      # Permite credenciais (cookies, cabeçalhos de autorização)
    allow_methods=["*"],         # Permite todos os métodos (GET, POST, etc.)
    allow_headers=["*"],         # Permite todos os cabeçalhos
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


def _dedupe_columns(names: List[str]) -> List[str]:
    """
    Deduplicate column names while preserving order: ["A", "A", ""] -> ["A", "A_2", "Column"]
    """
    counts: dict[str, int] = {}
    out: List[str] = []
    for raw in names:
        n = (raw or "").strip() or "Column"
        counts[n] = counts.get(n, 0) + 1
        if counts[n] > 1:
            out.append(f"{n}_{counts[n]}")
        else:
            out.append(n)
    return out


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
    Extract tables from each page of the PDF.
    Returns a list of (DataFrame, desired_sheet_name).
    """
    results: List[Tuple[pd.DataFrame, str]] = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        if not pdf.pages:
            return results

        for page_index, page in enumerate(pdf.pages, start=1):
            try:
                # USA O PARÂMETRO COLUMNS PARA EXTRAÇÃO FIXA
                # Isso ignora as regras de detecção de linhas e texto (vertical_strategy)
                # e força a separação da coluna "Turma" e a captura das 129 linhas.
                tables = page.extract_tables(table_settings={"columns": FIXED_COLUMN_COORDINATES}) or []
            except Exception as e:
                logger.exception("Unhandled error during PDF parsing and table extraction.")
                # Se o erro for de configuração, levantamos uma exceção 500 para não retornar 422
                raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                                    detail=f"Configuration Error in pdfplumber: {e}")


            for table_index, table in enumerate(tables, start=1):
                if not table:
                    continue

                # Filter out fully empty rows
                nonempty_rows = [
                    row for row in table
                    if row and any((cell is not None) and str(cell).strip() != "" for cell in row)
                ]
                if not nonempty_rows:
                    continue

                # Use the first row as header, which is the most practical general assumption.
                header_raw = [str(c).strip() if c is not None else "" for c in nonempty_rows[0]]
                header = _dedupe_columns(header_raw)

                # Normalize each row to the header size and build DataFrame
                normalized_rows = [_pad_or_truncate(row, len(header)) for row in nonempty_rows[1:]]
                df = pd.DataFrame(normalized_rows, columns=header)

                # Clean: drop fully empty rows/columns
                df.replace(r"^\s*$", pd.NA, regex=True, inplace=True)
                df.dropna(how="all", inplace=True)
                df.dropna(axis=1, how="all", inplace=True)

                # If data vanished (rare), keep at least the header-only dataframe
                if df.empty:
                    df = pd.DataFrame(columns=header)

                sheet_name = f"Table_{table_index}_Page_{page_index}"
                results.append((df, sheet_name))

    return results


# ------------------------------------------------------------------------------
# Routes
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
            for df, suggested_name in extracted:
                safe_name = sanitize_sheet_name(suggested_name, used_sheet_names)
                df.to_excel(writer, index=False, sheet_name=safe_name)
        output_buf.seek(0)
    except Exception:
        logger.exception("Failed creating the Excel workbook.")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail="An internal error occurred during file processing.")

    headers = {"Content-Disposition": f'attachment; filename="{ATTACHMENT_FILENAME}"'}
    return StreamingResponse(iter_bytesio(output_buf), media_type=EXCEL_MEDIA_TYPE, headers=headers)


@app.get("/health", tags=["health"])
def health() -> dict[str, str]:
    return {"status": "ok"}


if __name__ == "__main__":
    # For local development only. In containers, use the Dockerfile CMD.
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
