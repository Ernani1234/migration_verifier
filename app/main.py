import streamlit as st
import pandas as pd
from typing import Dict, Optional
from pathlib import Path
import sys, os, json

# Ensure parent directory (package root) is on sys.path for imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Local imports
from core.providers.sqlite_provider import SQLiteProvider
from core.providers.postgres_provider import PostgresProvider
from core.providers.mysql_provider import MySQLProvider
from core.providers.redshift_provider import RedshiftProvider
from core.providers.bigquery_provider import BigQueryProvider
from core.providers.snowflake_provider import SnowflakeProvider
from core.providers.s3_provider import S3Provider
from core.migration import migrate_table, check_table_quality, apply_corrections, add_data_from_dataframe


st.set_page_config(page_title="DeltaGuard", page_icon="🛡️", layout="wide")


def provider_selector(label: str) -> str:
    options = [
        "SQLite (local)",
        "Google BigQuery",
        "Amazon Redshift",
        "Snowflake",
        "PostgreSQL",
        "MySQL",
        "Amazon S3",
    ]
    return st.selectbox(label, options, index=0)


def render_credentials_form(provider_name: str, key_prefix: Optional[str] = None) -> Optional[Dict]:
    kp = key_prefix or provider_name.replace(" ", "_").lower()
    if provider_name == "SQLite (local)":
        st.caption("Forneça o caminho para o arquivo .sqlite/.db")
        db_path = st.text_input(
            "Caminho do banco SQLite",
            value=str(Path.cwd() / "data.sqlite"),
            key=f"{kp}_sqlite_db_path",
        )
        return {"db_path": db_path}
    elif provider_name == "PostgreSQL":
        st.caption("Informe credenciais do PostgreSQL")
        host = st.text_input("Host", key=f"{kp}_pg_host")
        port = st.number_input("Porta", value=5432, step=1, key=f"{kp}_pg_port")
        database = st.text_input("Database", key=f"{kp}_pg_db")
        user = st.text_input("Usuário", key=f"{kp}_pg_user")
        password = st.text_input("Senha", type="password", key=f"{kp}_pg_pwd")
        schema = st.text_input("Schema", value="public", key=f"{kp}_pg_schema")
        return {"host": host, "port": port, "database": database, "user": user, "password": password, "schema": schema}
    elif provider_name == "MySQL":
        st.caption("Informe credenciais do MySQL")
        host = st.text_input("Host", key=f"{kp}_my_host")
        port = st.number_input("Porta", value=3306, step=1, key=f"{kp}_my_port")
        database = st.text_input("Database", key=f"{kp}_my_db")
        user = st.text_input("Usuário", key=f"{kp}_my_user")
        password = st.text_input("Senha", type="password", key=f"{kp}_my_pwd")
        return {"host": host, "port": port, "database": database, "user": user, "password": password}
    elif provider_name == "Amazon Redshift":
        st.caption("Informe credenciais do Redshift")
        host = st.text_input("Host", key=f"{kp}_rs_host")
        port = st.number_input("Porta", value=5439, step=1, key=f"{kp}_rs_port")
        database = st.text_input("Database", key=f"{kp}_rs_db")
        user = st.text_input("Usuário", key=f"{kp}_rs_user")
        password = st.text_input("Senha", type="password", key=f"{kp}_rs_pwd")
        schema = st.text_input("Schema", value="public", key=f"{kp}_rs_schema")
        return {"host": host, "port": port, "database": database, "user": user, "password": password, "schema": schema}
    elif provider_name == "Google BigQuery":
        st.caption("Faça upload do credentials.json (Service Account)")
        keyfile_upload = st.file_uploader("credentials.json", type=["json"], key=f"{kp}_bq_keyfile")
        dataset = st.text_input("Dataset", key=f"{kp}_bq_dataset")
        if keyfile_upload is None:
            return None
        try:
            key_info = json.loads(keyfile_upload.getvalue().decode("utf-8"))
            project_id = key_info.get("project_id")
            # Salva arquivo temporário para o provider utilizar via caminho
            tmp_path = Path.cwd() / f"bq_{kp}_credentials.json"
            tmp_path.write_bytes(keyfile_upload.getvalue())
            return {"project_id": project_id, "dataset": dataset, "keyfile_path": str(tmp_path)}
        except Exception:
            st.error("Arquivo de credenciais inválido.")
            return None
    elif provider_name == "Snowflake":
        st.caption("Informe credenciais do Snowflake")
        account = st.text_input("Account", key=f"{kp}_sf_account")
        user = st.text_input("Usuário", key=f"{kp}_sf_user")
        password = st.text_input("Senha", type="password", key=f"{kp}_sf_pwd")
        warehouse = st.text_input("Warehouse", key=f"{kp}_sf_wh")
        database = st.text_input("Database", key=f"{kp}_sf_db")
        schema = st.text_input("Schema", key=f"{kp}_sf_schema")
        return {"account": account, "user": user, "password": password, "warehouse": warehouse, "database": database, "schema": schema}
    elif provider_name == "Amazon S3":
        st.caption("Faça upload do arquivo de credenciais AWS (~/.aws/credentials)")
        cred_file = st.file_uploader("credentials (INI)", type=["txt", "ini"], key=f"{kp}_s3_creds")
        bucket = st.text_input("Bucket", key=f"{kp}_s3_bucket")
        prefix = st.text_input("Prefixo (opcional)", key=f"{kp}_s3_prefix")
        if cred_file is None:
            return None
        try:
            import configparser
            ini = cred_file.getvalue().decode("utf-8")
            cp = configparser.ConfigParser()
            cp.read_string(ini)
            sect = cp["default"] if cp.has_section("default") else cp[cp.sections()[0]]
            creds = {
                "aws_access_key_id": sect.get("aws_access_key_id"),
                "aws_secret_access_key": sect.get("aws_secret_access_key"),
                "aws_session_token": sect.get("aws_session_token", None),
                "region": sect.get("region", None),
                "bucket": bucket,
                "prefix": prefix,
            }
            return creds
        except Exception:
            st.error("Arquivo de credenciais inválido.")
            return None
    else:
        st.warning("Conector não reconhecido.")
        return None


def make_provider(provider_name: str, creds: Dict):
    if provider_name == "SQLite (local)":
        return SQLiteProvider(creds)
    if provider_name == "PostgreSQL":
        return PostgresProvider(creds)
    if provider_name == "MySQL":
        return MySQLProvider(creds)
    if provider_name == "Amazon Redshift":
        return RedshiftProvider(creds)
    if provider_name == "Google BigQuery":
        return BigQueryProvider(creds)
    if provider_name == "Snowflake":
        return SnowflakeProvider(creds)
    if provider_name == "Amazon S3":
        return S3Provider(creds)
    else:
        return None


def render_transfer_animation(src_label: str, dst_label: str):
    st.markdown(
        """
        <style>
        .transfer-container {
            display: flex;
            align-items: center;
            justify-content: space-between;
            gap: 24px;
            padding: 24px 16px;
            border-radius: 16px;
            background: linear-gradient(135deg, #0e1117, #111827);
            border: 1px solid #2e3440;
        }
        .service {
            flex: 0 0 200px;
            height: 140px;
            border-radius: 16px;
            background: #151a24;
            display: flex;
            align-items: center;
            justify-content: center;
            color: #e5e7eb;
            font-weight: 600;
            font-size: 18px;
            border: 1px solid #2e3440;
            box-shadow: 0 6px 24px rgba(0,0,0,0.35);
        }
        .arrow-area {
            position: relative;
            flex: 1 1 auto;
            height: 120px;
        }
        .arrow {
            position: absolute;
            top: 50%;
            left: 0;
            right: 0;
            height: 6px;
            background: #374151;
            transform: translateY(-50%);
            border-radius: 6px;
            overflow: hidden;
        }
        .arrow:after {
            content: "";
            position: absolute;
            right: 0;
            top: -6px;
            width: 0;
            height: 0;
            border-top: 12px solid transparent;
            border-bottom: 12px solid transparent;
            border-left: 18px solid #4f46e5;
        }
        .packet {
            position: absolute;
            top: -6px;
            width: 18px;
            height: 18px;
            border-radius: 50%;
            background: linear-gradient(135deg, #60a5fa, #4f46e5);
            box-shadow: 0 4px 12px rgba(79,70,229,0.45);
            animation: move 2.2s linear infinite;
        }
        .packet.p2 { animation-delay: 0.5s; }
        .packet.p3 { animation-delay: 1.1s; }
        .packet.p4 { animation-delay: 1.6s; }
        @keyframes move {
            0% { left: -10%; opacity: 0; }
            10% { opacity: 1; }
            90% { opacity: 1; }
            100% { left: 100%; opacity: 0; }
        }
        </style>
        <div class="transfer-container">
          <div class="service">{src}</div>
          <div class="arrow-area">
            <div class="arrow">
              <div class="packet"></div>
              <div class="packet p2"></div>
              <div class="packet p3"></div>
              <div class="packet p4"></div>
            </div>
          </div>
          <div class="service">{dst}</div>
        </div>
        """.format(src=src_label, dst=dst_label), unsafe_allow_html=True,
    )


def render_loading_animation(label: str = "Analisando dados..."):
    st.markdown(
        f"""
        <style>
        .loader-wrap {{
            display: flex; align-items: center; gap: 12px;
            padding: 14px 16px; border-radius: 12px; border: 1px solid #2e3440;
            background: #0f1420; color: #e5e7eb; font-weight: 500;
        }}
        .loader {{
            width: 24px; height: 24px; border: 3px solid #374151;
            border-top-color: #22d3ee; border-radius: 50%;
            animation: spin 0.9s linear infinite;
        }}
        @keyframes spin {{ to {{ transform: rotate(360deg); }} }}
        </style>
        <div class="loader-wrap">
          <div class="loader"></div>
          <div>{label}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def page_migrate():
    st.header("Migrar dados")
    st.markdown(
        """
        <style>
        .vbar {width: 2px; background: #9ca3af; border-radius: 1px;}
        .stretch {height: 100%; min-height: 420px;}
        </style>
        """,
        unsafe_allow_html=True,
    )
    c1, cMid, c2 = st.columns([1, 0.02, 1])
    with c1:
        src_name = provider_selector("Serviço de origem")
        src_creds = render_credentials_form(src_name, key_prefix="src")
    with cMid:
        st.markdown('<div class="vbar stretch"></div>', unsafe_allow_html=True)
    with c2:
        dst_name = provider_selector("Serviço de destino")
        dst_creds = render_credentials_form(dst_name, key_prefix="dst")

    table_name = st.text_input("Nome da tabela para migrar", value="exemplo", key="migrate_table_name")
    do_migrate = st.button("Iniciar migração")

    if do_migrate:
        if not src_creds or not dst_creds:
            st.error("Preencha as credenciais necessárias.")
            return
        src = make_provider(src_name, src_creds)
        dst = make_provider(dst_name, dst_creds)
        if not src or not dst:
            st.warning("Selecione conectores compatíveis. SQLite está funcional nesta versão.")
            return

        ok_src = src.test_connection()
        ok_dst = dst.test_connection()
        if not ok_src or not ok_dst:
            st.error("Falha ao conectar ao serviço de origem ou destino.")
            return

        perms_src = src.has_permissions(["read"]) if hasattr(src, "has_permissions") else {"read": True}
        perms_dst = dst.has_permissions(["write"]) if hasattr(dst, "has_permissions") else {"write": True}
        if not perms_src.get("read"):
            st.error("Credencial de origem sem permissão de leitura.")
            return
        if not perms_dst.get("write"):
            st.error("Credencial de destino sem permissão de escrita.")
            return

        render_transfer_animation(src_name, dst_name)
        prog = st.progress(0)

        try:
            for i in range(0, 60, 10):
                prog.progress(i / 100)
            migrated_rows = migrate_table(src, dst, table_name)
            prog.progress(1.0)
            st.success(f"Migração concluída: {migrated_rows} linhas transferidas.")
        except Exception as e:
            st.error(f"Erro durante a migração: {e}")
        finally:
            src.close(); dst.close()


def page_check():
    st.header("Checar dados")
    name = provider_selector("Serviço")
    creds = render_credentials_form(name, key_prefix="check")
    table_name = st.text_input("Tabela para analisar", value="exemplo", key="check_table_name")
    run_check = st.button("Analisar")
    if run_check:
        if not creds:
            st.error("Preencha as credenciais necessárias.")
            return
        prov = make_provider(name, creds)
        if not prov:
            st.warning("Conector não disponível. Utilize SQLite nesta versão.")
            return
        if not prov.test_connection():
            st.error("Falha ao conectar.")
            return
        render_loading_animation()
        try:
            report = check_table_quality(prov, table_name)
            st.subheader("Resultado da análise")
            st.write(report)
            if report.get("duplicates", 0) > 0:
                if st.button("Aplicar correções (remover duplicados)"):
                    perms = prov.has_permissions(["write"]) if hasattr(prov, "has_permissions") else {"write": True}
                    if not perms.get("write"):
                        st.warning("Credencial sem permissão de escrita para aplicar correções.")
                    else:
                        fixed = apply_corrections(prov, table_name)
                        st.success(f"Correções aplicadas. Removidos {fixed} duplicados.")
        except Exception as e:
            st.error(f"Erro na análise: {e}")
        finally:
            prov.close()


def page_add():
    st.header("Adicionar dados")
    name = provider_selector("Serviço")
    creds = render_credentials_form(name, key_prefix="add")
    table_name = st.text_input("Nome da tabela de destino", value="novo_dados", key="add_table_name")
    file = st.file_uploader("Arquivo (CSV ou Excel)", type=["csv", "xlsx"])
    do_add = st.button("Adicionar")
    if do_add:
        if not creds:
            st.error("Preencha as credenciais necessárias.")
            return
        prov = make_provider(name, creds)
        if not prov or not prov.test_connection():
            st.error("Falha ao conectar.")
            return
        if not file:
            st.error("Envie um arquivo válido.")
            return
        try:
            if file.name.endswith(".csv"):
                df = pd.read_csv(file)
            else:
                df = pd.read_excel(file)
            st.write("Pré-visualização:", df.head())
            rows = add_data_from_dataframe(prov, table_name, df)
            st.success(f"Dados adicionados com sucesso: {rows} linhas.")
        except Exception as e:
            st.error(f"Erro ao adicionar dados: {e}")
        finally:
            prov.close()


def page_delete():
    st.header("Excluir dados")
    name = provider_selector("Serviço")
    creds = render_credentials_form(name, key_prefix="delete")
    if not creds:
        st.stop()
    prov = make_provider(name, creds)
    if not prov or not prov.test_connection():
        st.error("Falha ao conectar.")
        st.stop()

    tables = prov.list_tables()
    if not tables:
        st.info("Nenhuma tabela encontrada.")
        prov.close()
        st.stop()
    table = st.selectbox("Tabela", tables)
    st.write("Amostra:")
    try:
        df = prov.read_table(table, limit=50)
        st.dataframe(df)
    except Exception:
        st.info("Não foi possível ler a amostra da tabela.")

    c1, c2 = st.columns(2)
    with c1:
        if st.button("Excluir tabela inteira", type="secondary"):
            perms = prov.has_permissions(["write"]) if hasattr(prov, "has_permissions") else {"write": True}
            if not perms.get("write"):
                st.warning("Credencial sem permissão de escrita para excluir.")
            else:
                prov.delete_table(table)
                st.success("Tabela excluída.")
    with c2:
        where = st.text_input("Cláusula WHERE para excluir linhas (opcional)", value="", key="delete_where_clause")
        if st.button("Excluir linhas"):
            perms = prov.has_permissions(["write"]) if hasattr(prov, "has_permissions") else {"write": True}
            if not perms.get("write"):
                st.warning("Credencial sem permissão de escrita para excluir.")
            else:
                deleted = prov.delete_rows(table, where if where.strip() else None)
                st.success(f"{deleted} linhas excluídas.")
    prov.close()


def main():
    st.title("DeltaGuard")
    st.caption("Dados em movimento, confiança garantida — migre, valide e avance.")
    # Oculta o ícone de link que aparece ao lado dos títulos ao passar o mouse
    st.markdown(
        """
        <style>
        h1 a, h2 a, h3 a, h4 a, h5 a, h6 a { display: none !important; visibility: hidden !important; }
        </style>
        """,
        unsafe_allow_html=True,
    )

    page = st.sidebar.radio(
        "Objetivo",
        ["Migrar dados", "Checar dados", "Adicionar dados", "Excluir dados"],
        index=0,
    )

    if page == "Migrar dados":
        page_migrate()
    elif page == "Checar dados":
        page_check()
    elif page == "Adicionar dados":
        page_add()
    else:
        page_delete()


if __name__ == "__main__":
    main()