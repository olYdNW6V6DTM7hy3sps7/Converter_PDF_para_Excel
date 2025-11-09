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
# Com base na análise do PDF, a tabela tem 3 colunas de dados.
# Definimos 4 linhas verticais para criar 3 colunas.
FIXED_COLUMN_COORDINATES = [
    30,  # Início real da tabela (Nome do Aluno)
    250, # Fim da Coluna 'Aluno' / Início da Coluna 'Telefone'
    390, # Fim da Coluna 'Telefone' / Início da Coluna 'Responsável'
    580  # Fim da Coluna 'Responsável'
]

# CONFIGURAÇÃO DE EXTRAÇÃO COMPATÍVEL COM pdfplumber==0.11.8
# Força o uso das coordenadas X acima.
TABLE_SETTINGS_V0_11_8 = {
    "explicit_vertical_lines": FIXED_COLUMN_COORDINATES,
    "vertical_strategy": "text", 
    "snap_tolerance": 5, 
    "join_tolerance": 5,
}

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


def _dedupe_columns(names: List[str]) -> List[str]:
    # Mantido para compatibilidade, mas não usado na nova lógica de extração
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
    
    # Cabeçalho limpo e FIXO que deve ser usado para todas as sheets.
    MANUAL_CLEAN_HEADERS = ["Turma", "Aluno", "Telefone", "Responsável"]
    NUM_DATA_COLUMNS = len(MANUAL_CLEAN_HEADERS) - 1 # Exclui 'Turma' da contagem de dados extraídos

    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        if not pdf.pages:
            return results

        for page_index, page in enumerate(pdf.pages, start=1):
            
            # 1. Capturar o nome da Turma (primeira linha da página)
            page_text = page.extract_text(x_tolerance=2)
            sheet_name_base = f"Page_{page_index}" # Default seguro
            class_name = ""
            
            if page_text:
                first_line = page_text.split('\n')[0].strip()
                # Tenta capturar o nome da turma (e remove a palavra "Propedeutica" que estava sobrando no PDF)
                match = re.search(r"^\dº\s+(.*)", first_line)
                if match:
                    class_name = match.group(1).strip().replace("Propedeutica", "").strip()
                    sheet_name_base = class_name
                # Se não encontrar, tenta usar a linha toda (ex: "3º Logística")
                elif re.match(r"^\dº\s+", first_line):
                    class_name = first_line
                    sheet_name_base = class_name

            try:
                # 2. Extrair a tabela usando coordenadas fixas e sintaxe legada
                tables = page.extract_tables(table_settings=TABLE_SETTINGS_V0_11_8) or []
            except Exception as e:
                logger.exception("Unhandled error during PDF parsing and table extraction.")
                raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                                    detail=f"Configuration Error in pdfplumber: {e}")

            # Vamos tratar apenas a primeira tabela detectada, pois a lista é um grande bloco
            if not tables:
                continue
            
            table = tables[0]

            # Filter out rows that are completely empty
            nonempty_rows = [
                row for row in table
                if row and any((cell is not None) and str(cell).strip() != "" for cell in row)
            ]
            if not nonempty_rows:
                continue

            # 3. Limpar dados e montar o DataFrame
            
            # PULA A PRIMEIRA LINHA: A primeira linha sempre contém o cabeçalho ruidoso ("Aluno","Telefone","Responsável").
            data_rows = nonempty_rows[1:]
            
            if not data_rows:
                continue # Nada de dados para processar

            # Garante que cada linha tenha o número correto de colunas de DADOS (3)
            normalized_rows = [_pad_or_truncate(row, NUM_DATA_COLUMNS) for row in data_rows]
            
            # Cria o DataFrame usando o cabeçalho de 3 colunas (Aluno, Telefone, Responsável)
            df = pd.DataFrame(normalized_rows, columns=MANUAL_CLEAN_HEADERS[1:])

            # Adiciona a coluna 'Turma' (Class Name) como a primeira coluna
            df.insert(0, "Turma", class_name if class_name else f"Página {page_index}")
            
            # Limpeza final
            df.replace(r"^\s*$", pd.NA, regex=True, inplace=True)
            df.dropna(how="all", inplace=True)
            
            # Se todas as colunas de dados (menos a Turma) estiverem vazias, remove a linha
            df.dropna(subset=MANUAL_CLEAN_HEADERS[1:], how="all", inplace=True)


            # Usa o nome da turma para a sheet
            # IMPORTANTE: Para consolidar os 129 contatos em um único Excel,
            # precisamos garantir que os nomes das sheets sejam únicos, mas consistentes.
            # Vou manter o nome da turma como nome da sheet,
            # confiando na função sanitize_sheet_name para lidar com duplicatas.
            results.append((df, sheet_name_base))

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
            # Novo: Concatena todos os DataFrames em um único DF para a sheet "Geral"
            # Isso garante que todos os 129 contatos estejam juntos.
            all_dfs = [df for df, _ in extracted]
            
            # Se o usuário quer a 'Turma' como coluna, faz mais sentido ter uma aba "Geral"
            # com todos os dados, e abas extras por turma (se necessário)
            # Vamos simplificar para apenas uma aba "Geral" para os 129 contatos.
            if all_dfs:
                df_general = pd.concat(all_dfs, ignore_index=True)
                # Garante que a coluna 'Turma' seja a primeira
                cols = ['Turma'] + [col for col in df_general.columns if col != 'Turma']
                df_general = df_general[cols]

                safe_name = sanitize_sheet_name("Geral - Contatos", used_sheet_names)
                df_general.to_excel(writer, index=False, sheet_name=safe_name)
            
            # Se quisermos sheets separadas por turma (como o código anterior faria),
            # o bloco abaixo é necessário, mas optamos por uma sheet "Geral" para todos os 129.
            # for df, suggested_name in extracted:
            #     safe_name = sanitize_sheet_name(suggested_name, used_sheet_names)
            #     df.to_excel(writer, index=False, sheet_name=safe_name)

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
