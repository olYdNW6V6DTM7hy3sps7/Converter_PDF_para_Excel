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

# Coordenadas X para agrupamento (banding) baseadas no seu PDF (listas_completas_v2.pdf)
# O texto é separado por coordenada X: [Left boundary, Column 1 split, Column 2 split, Right boundary]
# [0] = Início, [1] = Fim do Aluno/Início do Telefone, [2] = Fim do Telefone/Início do Responsável, [3] = Fim do Responsável
COLUMN_BANDING_X = [20, 200, 310, 580] # Valores ajustados para o seu PDF.

# Regex para limpar ruído (aspas, quebras de linha, símbolos não essenciais)
CLEAN_TEXT_REGEX = re.compile(r'[^a-zA-Z0-9\s\(\)\-\+\.\sáéíóúÁÉÍÓÚãõÃÕçÇ:]')

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


def _get_column_index(x_coord: float) -> int:
    """
    Determina o índice da coluna (0, 1, ou 2) com base na coordenada X.
    """
    if COLUMN_BANDING_X[0] <= x_coord < COLUMN_BANDING_X[1]:
        return 0  # Aluno
    elif COLUMN_BANDING_X[1] <= x_coord < COLUMN_BANDING_X[2]:
        return 1  # Telefone
    elif COLUMN_BANDING_X[2] <= x_coord < COLUMN_BANDING_X[3]:
        return 2  # Responsável
    return -1 # Fora da área de interesse


def extract_tables_from_pdf(pdf_bytes: bytes) -> List[Tuple[pd.DataFrame, str]]:
    """
    Extrai dados do PDF usando coordenadas de palavras (abordagem "pixel a pixel").
    """
    results: List[Tuple[pd.DataFrame, str]] = []
    
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        if not pdf.pages:
            return results

        for page_index, page in enumerate(pdf.pages, start=1):
            
            # 1. Capturar o nome da Turma (do topo da página)
            class_name = ""
            sheet_name_base = f"Page_{page_index}"
            
            # Extrai o texto da área superior onde o nome da turma geralmente está
            try:
                # Cria um corte na parte superior da página (Y=0 a Y=70)
                top_area = page.crop((0, 0, page.width, 70)) 
                top_text = top_area.extract_text().strip()
                
                if top_text:
                    # Tenta encontrar a linha que se parece com o nome de uma turma ("3º NomeDaTurma")
                    match_turma = re.search(r"^\dº\s+(.*)", top_text)
                    if match_turma:
                        class_name = match_turma.group(1).strip().split('\n')[0].strip()
                        sheet_name_base = class_name
            except Exception:
                 # Se o corte falhar, a turma fica em branco
                pass
            
            # 2. Extrair todas as palavras com coordenadas e agrupá-las por linha (Y) e coluna (X)
            
            # words: lista de dicionários com 'text', 'x0', 'y0', 'x1', 'y1', 'doctop', 'top', 'bottom'
            words = page.extract_words(keep_blank_chars=False)
            
            # Agrupar as palavras pela coordenada Y (linha)
            rows: Dict[int, List[Dict]] = {}
            for word in words:
                # Usa a coordenada Y do topo para definir a linha
                y_coord = int(word["top"]) 
                
                # Ignora texto na área superior (onde está o nome da turma e o cabeçalho ruidoso)
                if y_coord < 70: 
                    continue
                    
                if y_coord not in rows:
                    rows[y_coord] = []
                rows[y_coord].append(word)

            
            extracted_data = []

            # 3. Processar cada linha de palavras
            for y_coord in sorted(rows.keys()):
                
                # Inicializa a linha com 3 strings vazias (Aluno, Telefone, Responsável)
                current_row = [""] * NUM_DATA_COLUMNS
                
                # Ordena as palavras dentro da linha pela coordenada X
                words_in_row = sorted(rows[y_coord], key=lambda w: w["x0"])
                
                # Itera sobre as palavras e as insere na coluna correta
                for word in words_in_row:
                    col_index = _get_column_index(word["x0"])
                    
                    if col_index != -1:
                        # Adiciona a palavra à string da coluna, separada por espaço
                        if current_row[col_index]:
                            current_row[col_index] += " " + word["text"]
                        else:
                            current_row[col_index] = word["text"]

                # Limpeza final e adiciona aos dados extraídos
                if any(current_row): # Se a linha não estiver completamente vazia
                    # Remove o ruído (aspas, vírgulas, quebras de linha que sobraram, etc.)
                    cleaned_row = [CLEAN_TEXT_REGEX.sub('', cell).strip() for cell in current_row]
                    
                    # Certifica-se de que o primeiro campo (Aluno) não é o cabeçalho ruidoso
                    if cleaned_row[0] and cleaned_row[0].lower() not in ["aluno", "responsável", "telefone"]:
                         extracted_data.append(cleaned_row)

            
            if not extracted_data:
                continue

            # 4. Criar o DataFrame
            df = pd.DataFrame(extracted_data, columns=MANUAL_CLEAN_HEADERS[1:])

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
