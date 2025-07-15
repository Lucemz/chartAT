"""
analytics.py
~~~~~~~~~~~~
Herramientas rápidas para pivot-tables y métricas de eventos AppsFlyer.
Ahora admite selección interactiva de archivo si no se pasa ruta por CLI.

Uso rápido:
    python analytics.py                    # abre diálogo para elegir CSV
    python analytics.py file.csv --daily   # como antes, pasando la ruta
"""

from __future__ import annotations
import json
from pathlib import Path
import pandas as pd
import logging
import sys
# Modo dashboard: activamos si hay Streamlit disponible
try:
    import streamlit as st
    import matplotlib.pyplot as plt
    IS_STREAMLIT = True
    from contextlib import contextmanager
except ModuleNotFoundError:
    IS_STREAMLIT = False
# ──────────────────────────────────────────────────────────────────────────
# Soporte de pie interactivas con Plotly + streamlit‑plotly‑events
try:
    from streamlit_plotly_events import plotly_events
    import plotly.graph_objects as go
    HAS_PLOTLY_EVENTS = True
except ModuleNotFoundError:
    HAS_PLOTLY_EVENTS = False
# ------------------------------------------------------------------
# Modal seguro: usa st.modal si la versión lo soporta, si no muestra
# un sub‑encabezado como fallback.
# ------------------------------------------------------------------
@contextmanager
def _safe_modal(title: str):
    if hasattr(st, "modal"):
        with st.modal(title):
            yield
    else:
        st.subheader(title)
        yield
        st.info("Desplázate para cerrar el detalle.")
# ──────────────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')

# ──────────────────────────────────────────────────────────────────────────────
# 0. Utilidad para escoger un archivo si no se pasa por CLI
# ──────────────────────────────────────────────────────────────────────────────
def pick_file() -> str:
    """
    Devuelve la ruta al CSV elegido por el usuario.
    1) Intenta abrir un diálogo gráfico con tkinter.
    2) Si tkinter no está disponible o falla, pide la ruta por stdin.
    """
    try:
        import tkinter as tk
        from tkinter import filedialog
        root = tk.Tk()
        root.withdraw()                     # oculta la ventana principal
        path = filedialog.askopenfilename(
            title="Seleccione el CSV de AppsFlyer",
            filetypes=[("Archivos CSV", "*.csv"), ("Todos los archivos", "*.*")],
        )
        logging.info(f"Archivo seleccionado: {path}")
        root.destroy()
        if not path:
            raise SystemExit("✗ No se seleccionó ningún archivo.")
        return path
    except Exception:
        # Fallback: entrada manual
        return input("Ruta al CSV de AppsFlyer: ").strip()

# ──────────────────────────────────────────────────────────────────────────────
# 1. Carga y limpieza (sin cambios)
# ──────────────────────────────────────────────────────────────────────────────
def load(path: str | Path,
         user_col: str = "Customer User ID",
         parse_dates: tuple[str, ...] = ("Event Time",)) -> pd.DataFrame:
    df = pd.read_csv(path, low_memory=False)
    for col in parse_dates:
        if col in df:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    df["date"] = df["Event Time"].dt.date
    df["hour"] = df["Event Time"].dt.hour

    # # Extraer 'ud_flow' del JSON en la columna 'Event Value'
    # def _extract_ud_flow(value):
    #     """
    #     Devuelve 'login', 'registro', u otra cadena si existe la
    #     clave "ud_flow" en el JSON del Event Value; de lo contrario, None.
    #     """
    #     if pd.isna(value):
    #         return None
    #     try:
    #         parsed = json.loads(value)
    #         if isinstance(parsed, dict):
    #             return parsed.get("ud_flow")
    #     except (ValueError, TypeError):
    #         pass
    #     return None
    #
    # df["ud_flow"] = df["Event Value"].apply(_extract_ud_flow)

    if user_col not in df.columns:
        raise KeyError(f"No encuentro la columna '{user_col}' en el CSV")
    return df

# ──────────────────────────────────────────────────────────────────────────────
# 1.b Menú interactivo cuando no se pasan flags
# ──────────────────────────────────────────────────────────────────────────────
def interactive_menu(df: pd.DataFrame) -> None:
    """
    Presenta un menú textual con las operaciones más comunes.
    Se repite hasta que el usuario elija “0” (salir).
    """
    while True:
        print(
            "\n=== Seleccione operación ===\n"
            "1) Totales por tipo de evento\n"
            "2) Pivot diario (totales)\n"
            "3) Pivot diario (usuarios únicos)\n"
            "4) Ratio error‑login → login OK\n"
            "5) Ratio error‑registro → registro OK\n"
            "6) Historial de un usuario específico\n"
            "0) Salir"
        )
        choice = input("Opción: ").strip()
        if choice == "1":
            print(event_counts(df), "\n")
        elif choice == "2":
            print(daily_pivot(df), "\n")
        elif choice == "3":
            print(daily_pivot(df, value="Customer User ID", agg="nunique"), "\n")
        elif choice == "4":
            print("Login error → login OK ratio:", login_error_success_ratio(df))
        elif choice == "5":
            print("Registro error → registro OK ratio:", registration_error_success_ratio(df))
        elif choice == "6":
            user_id = input("ID de usuario: ").strip()
            hist = user_history(df, user_id)
            if hist.empty:
                print(f"✗ No hay eventos para el usuario {user_id}")
            else:
                print(hist.to_string(index=False))
        elif choice == "0":
            logging.info("Menú interactivo finalizado.")
            break
        else:
            print("Opción no válida, intenta de nuevo.")

# ──────────────────────────────────────────────────────────────────────────────
# 1.bis Clasificación de usuarios por flujo (error/éxito)
# ──────────────────────────────────────────────────────────────────────────────
def _categorize_flow(df: pd.DataFrame,
                     err_mask: pd.Series,
                     ok_mask: pd.Series,
                     user_col: str = "Customer User ID") -> dict:
    """
    Devuelve un diccionario con:
      error_then_ok: # usuarios con error y luego éxito
      error_only:    # usuarios que solo tuvieron error
      clean:         # usuarios que nunca tuvieron error
      total_errors:  # total de eventos de error
      total_ok:      # total de eventos de éxito
    """
    err_users  = set(df[err_mask][user_col])
    ok_users   = set(df[ok_mask][user_col])

    both_users        = err_users & ok_users
    only_error_users  = err_users - ok_users
    only_success_users = ok_users - err_users

    # Orden temporal para “error→éxito”
    error_then_success_users = set()
    if not df.empty and both_users:
        first_times = (
            df
            .loc[df[user_col].isin(both_users), [user_col, "Event Time", "Event Name"]]
            .sort_values("Event Time")
        )
        for u in both_users:
            first_err = first_times.loc[
                (first_times[user_col] == u) & err_mask, "Event Time"
            ].min()
            first_ok = first_times.loc[
                (first_times[user_col] == u) & ok_mask, "Event Time"
            ].min()
            if pd.notna(first_err) and pd.notna(first_ok) and first_ok > first_err:
                error_then_success_users.add(u)

    total_errors = int(err_mask.sum())
    total_ok     = int(ok_mask.sum())

    return {
        # eventos
        "total_events" : total_errors + total_ok,
        "total_ok"     : total_ok,
        "total_errors" : total_errors,
        # usuarios
        "total_users"        : len(err_users | ok_users),
        "unique_ok_users"    : len(ok_users),
        "unique_error_users" : len(err_users),
        "both"        : len(both_users),      # tuvo éxito y error (cualquier orden)
        "only_error"  : len(only_error_users),
        "only_success": len(only_success_users),
        "only_error_ids": only_error_users,
        "only_success_ids": only_success_users,
        "both_ids": both_users,
        # claves legacy (compatibilidad)
        "error_then_ok": len(both_users),
        "error_only"   : len(only_error_users),
        "clean"        : len(only_success_users),
        "error_then_success": len(error_then_success_users),
    }

# ──────────────────────────────────────────────────────────────────────────────
# 1.c Métricas de navegación
# ──────────────────────────────────────────────────────────────────────────────
def navigation_stats(df: pd.DataFrame, user_col: str = "Customer User ID") -> pd.DataFrame:
    """
    Devuelve un DataFrame con:
        category         eventos  usuarios  ratio_eventos  ratio_usuarios
    """
    # Tomamos filas cuyo nombre de evento contenga “nav” (case‑insensitive)
    nav_mask = df["Event Name"].str.contains("nav", case=False, na=False)
    if nav_mask.sum() == 0:
        return pd.DataFrame()
    nav_df = df[nav_mask].copy()

    # Determinar categoría por keywords
    def _cat(row):
        name = row["Event Name"].lower()
        if "bottom" in name:
            return "bottom"
        elif "top" in name:
            return "top"
        elif "hamburger" in name:
            return "hamburger"
        elif "userprofile" in name or "profile" in name:
            return "userprofile"
        else:
            return "other"

    nav_df["category"] = nav_df.apply(_cat, axis=1)
    # Métricas
    events = nav_df.groupby("category")["Event Name"].count()
    users = nav_df.groupby("category")[user_col].nunique()
    total_events = events.sum()
    total_users = users.sum()
    ratios_e = events / total_events
    ratios_u = users / total_users
    result = (
        pd.DataFrame({
            "eventos": events,
            "usuarios": users,
            "ratio_eventos": ratios_e.round(3),
            "ratio_usuarios": ratios_u.round(3),
        })
        .reset_index()
        .rename(columns={"index": "category"})
    )
    return result

# ──────────────────────────────────────────────────────────────────────────────
# 2. Métricas, 3. Ratios, 4. Pivots, 5. CLI (idénticos salvo el parser)
# ──────────────────────────────────────────────────────────────────────────────
def event_counts(df: pd.DataFrame) -> pd.Series:
    return df["Event Name"].value_counts()

def most_common_event(df: pd.DataFrame) -> str:
    return event_counts(df).idxmax()

def _ratio_error_then_ok(df: pd.DataFrame,
                         error_mask: pd.Series,
                         ok_mask: pd.Series,
                         user_col: str = "Customer User ID") -> float:
    err = (df[error_mask]
           .sort_values("Event Time")
           .groupby(user_col)["Event Time"].first())
    ok = (df[ok_mask]
          .sort_values("Event Time")
          .groupby(user_col)["Event Time"].first())
    comunes = err.index.intersection(ok.index)
    sucesos = (ok.loc[comunes] > err.loc[comunes]).sum()
    return sucesos / len(err) if len(err) else float("nan")

def login_error_success_ratio(df: pd.DataFrame) -> float:
    err_mask = (
        df["Event Name"].str.lower().eq("ud_error")
        & df["Event Value"].str.contains('"ud_flow":"login"', case=False, na=False)
    )
    ok_mask = df["Event Name"].str.lower().eq("af_login")
    return _ratio_error_then_ok(df, err_mask, ok_mask)

def registration_error_success_ratio(df: pd.DataFrame) -> float:
    err_mask = (
        df["Event Name"].str.lower().eq("ud_error")
        & df["Event Value"].str.contains('"ud_flow":"registro"', case=False, na=False)
    )
    ok_mask = df["Event Name"].str.lower().eq("af_complete_registration")
    return _ratio_error_then_ok(df, err_mask, ok_mask)

def registration_stats(df: pd.DataFrame, user_col: str = "Customer User ID") -> dict:
    err_mask = (
        df["Event Name"].str.lower().eq("ud_error")
        & df["Event Value"].str.contains('"ud_flow":"registro"', case=False, na=False)
    )
    ok_mask  = df["Event Name"].str.lower().eq("af_complete_registration")
    return _categorize_flow(df, err_mask, ok_mask, user_col=user_col)

def login_stats(df: pd.DataFrame, user_col: str = "Customer User ID") -> dict:
    err_mask = (
        df["Event Name"].str.lower().eq("ud_error")
        & df["Event Value"].str.contains('"ud_flow":"login"', case=False, na=False)
    )
    ok_mask  = df["Event Name"].str.lower().eq("af_login")
    return _categorize_flow(df, err_mask, ok_mask, user_col=user_col)

def daily_pivot(df: pd.DataFrame,
                value: str = "AppsFlyer ID",
                agg: str | callable = "count") -> pd.DataFrame:
    return (df
            .pivot_table(index="date",
                         columns="Event Name",
                         values=value,
                         aggfunc=agg)
            .fillna(0)
            .astype(int))

def user_history(df: pd.DataFrame,
                 user_id: str,
                 user_col: str = "Customer User ID") -> pd.DataFrame:
    cols = ["Event Time", "Event Name", "ud_flow", "Event Value"]
    return (df[df[user_col] == user_id]
            .sort_values("Event Time")[cols]
            .reset_index(drop=True))

# ──────────────────────────────────────────────────────────────────────────────
# 1.c Dashboard Streamlit
# ──────────────────────────────────────────────────────────────────────────────
def _plot_pie(
    values,
    labels,
    title: str,
    *,
    masks: dict[str, pd.Series] | None = None,
    df: pd.DataFrame | None = None,
    user_col: str,
):
    """
    Dibuja un gráfico de pie.  Si está disponible la librería
    `streamlit_plotly_events`, el gráfico es interactivo: al hacer clic sobre
    un sector se abre un modal con la tabla `Event Value` y su contador.

    Parámetros
    ----------
    values : list‑like
        Valores numéricos para cada sector.
    labels : list‑like
        Etiquetas de los sectores (deben coincidir con las claves de `masks`).
    title : str
        Título del gráfico.
    masks : dict[label, boolean‑mask], opcional
        Máscaras booleanas (mismo tamaño que `df`) que definen qué filas
        pertenecen a cada etiqueta/sector.
    df : DataFrame, opcional
        DataFrame original para crear el detalle en la modal.
    """
    if HAS_PLOTLY_EVENTS:
        fig = go.Figure(
            data=[
                go.Pie(
                    labels=labels,
                    values=values,
                    hoverinfo="label+percent",
                )
            ]
        )
        fig.update_layout(title_text=title)
        selected = plotly_events(
            fig,
            click_event=True,
            select_event=False,
            hover_event=False,
            key=f"pie_{title}",
        )
        if selected and masks and df is not None:
            idx = selected[0].get("pointIndex", selected[0].get("pointNumber"))
            sel_label = labels[idx]
            sel_mask = masks.get(sel_label)
            if sel_mask is not None:
                subset = df.loc[sel_mask].copy()
                # Keep only the last event per user for this category
                last_per_user = (
                    subset.sort_values("Event Time")
                          .drop_duplicates(subset=[user_col], keep="last")
                )
                counts = (
                    last_per_user["Event Value"]
                    .value_counts(dropna=False)
                    .rename("Count")
                    .reset_index()
                    .rename(columns={"index": "Event Value"})
                )
                with _safe_modal(f"{title} – {sel_label}"):
                    st.dataframe(counts)
    else:
        fig, ax = plt.subplots()
        ax.pie(values, labels=labels, autopct="%1.1f%%")
        ax.set_title(title)
        st.pyplot(fig)
        st.warning(
            "Interactividad deshabilitada: instala 'streamlit-plotly-events' para activarla."
        )

# ──────────────────────────────────────────────────────────────────────────
# Útil para generar máscaras de error/éxito según flujo
# ──────────────────────────────────────────────────────────────────────────
def _get_flow_masks(
    df: pd.DataFrame, flow: str, user_col: str = "Customer User ID"
) -> tuple[pd.Series, pd.Series]:
    """
    Devuelve (err_mask, ok_mask) para el flujo indicado ('registro' o 'login').
    """
    flow = flow.lower()
    if flow == "registro":
        err_mask = (
            df["Event Name"].str.lower().eq("ud_error")
            & df["Event Value"].str.contains('"ud_flow":"registro"', case=False, na=False)
        )
        ok_mask = df["Event Name"].str.lower().eq("af_complete_registration")
    elif flow == "login":
        err_mask = (
            df["Event Name"].str.lower().eq("ud_error")
            & df["Event Value"].str.contains('"ud_flow":"login"', case=False, na=False)
        )
        ok_mask = df["Event Name"].str.lower().eq("af_login")
    else:
        raise ValueError("flow debe ser 'registro' o 'login'")
    return err_mask, ok_mask

def build_dashboard():
    st.title("Dashboard de eventos AppsFlyer")
    uploaded_file = st.sidebar.file_uploader("Sube tu CSV de AppsFlyer", type="csv")
    if uploaded_file is None:
        st.info("↖️ Sube un archivo para comenzar.")
        return
    df = load(uploaded_file)
    st.success(f"Archivo cargado: {len(df)} filas, {len(df.columns)} columnas")

    candidate_cols = [c for c in df.columns if ("ID" in c or "User" in c)]
    # Preferir AppsFlyer ID como opción por defecto si existe
    if "AppsFlyer ID" in candidate_cols:
        default_idx = candidate_cols.index("AppsFlyer ID")
    elif "Customer User ID" in candidate_cols:
        default_idx = candidate_cols.index("Customer User ID")
    else:
        default_idx = 0
    user_col = st.sidebar.selectbox("Columna de usuario", candidate_cols, index=default_idx)

    section = st.sidebar.radio("Análisis", ("Registro", "Login", "Navegación"))
    if section == "Registro":
        stats = registration_stats(df, user_col=user_col)
        err_mask, ok_mask = _get_flow_masks(df, "registro", user_col=user_col)
        # Definimos máscaras para drill-down correcto
        masks = {
            "Solo éxito": ok_mask & df[user_col].isin(stats["only_success_ids"]),
            "Solo error": err_mask & df[user_col].isin(stats["only_error_ids"]),
            "Éxito+Error": err_mask & df[user_col].isin(stats["both_ids"]),
        }
        _plot_pie(
            [stats["only_success"], stats["only_error"], stats["both"]],
            ["Solo éxito", "Solo error", "Éxito+Error"],
            "Registro – usuarios",
            masks=masks,
            df=df,
            user_col=user_col,
        )

        st.subheader("Eventos")
        colA, colB, colC = st.columns(3)
        colA.metric("Total eventos", stats["total_events"])
        colB.metric("Eventos OK",    stats["total_ok"])
        colC.metric("Eventos error", stats["total_errors"])

        st.subheader("Usuarios")
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Totales",      stats["total_users"])
        col2.metric("Solo éxito",   stats["only_success"])
        col3.metric("Solo error",   stats["only_error"])
        col4.metric("Éxito+Error",  stats["both"])
        # st.caption(f"{stats['only_error']/stats['total_users']:.1%} de los usuarios tuvo al menos un error; "
        #            f"{stats['only_success']/stats['total_users']:.1%} nunca presentó errores.")
        # Modal de errores de registro
        if "error" in stats and stats["only_error"] > 0:
            with _safe_modal("Errores de registro"):
                st.write("Detalle de errores de registro (implementa aquí si es necesario).")

    elif section == "Login":
        stats = login_stats(df, user_col=user_col)
        err_mask, ok_mask = _get_flow_masks(df, "login", user_col=user_col)
        # Definimos máscaras para drill-down correcto
        masks = {
            "Solo éxito": ok_mask & df[user_col].isin(stats["only_success_ids"]),
            "Solo error": err_mask & df[user_col].isin(stats["only_error_ids"]),
            "Éxito+Error": err_mask & df[user_col].isin(stats["both_ids"]),
        }
        _plot_pie(
            [stats["only_success"], stats["only_error"], stats["both"]],
            ["Solo éxito", "Solo error", "Éxito+Error"],
            "Login – usuarios",
            masks=masks,
            df=df,
            user_col=user_col,
        )

        st.subheader("Eventos")
        colA, colB, colC = st.columns(3)
        colA.metric("Total eventos", stats["total_events"])
        colB.metric("Eventos OK",    stats["total_ok"])
        colC.metric("Eventos error", stats["total_errors"])

        st.subheader("Usuarios")
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Totales",      stats["total_users"])
        col2.metric("Solo éxito",   stats["only_success"])
        col3.metric("Solo error",   stats["only_error"])
        col4.metric("Éxito+Error",  stats["both"])
        # st.caption(f"{stats['only_error']/stats['total_users']:.1%} de los usuarios tuvo al menos un error; "
        #            f"{stats['only_success']/stats['total_users']:.1%} nunca presentó errores.")
        # Modal de errores de login
        if "error" in stats and stats["only_error"] > 0:
            with _safe_modal("Errores de login"):
                st.write("Detalle de errores de login (implementa aquí si es necesario).")
    else:
        st.subheader("Navegación – métricas")
        nav_df = navigation_stats(df, user_col=user_col)
        if nav_df.empty:
            st.info("No se encontraron eventos de navegación (contengan 'nav').")
        else:
            st.metric("Usuarios únicos", int(nav_df["usuarios"].sum()))
            st.metric("Eventos totales", int(nav_df["eventos"].sum()))
            st.bar_chart(nav_df.set_index("category")["eventos"])
            st.dataframe(nav_df)

if __name__ == "__main__":
    import argparse, textwrap, sys
    # Si se ejecuta con Streamlit (`streamlit run main.py`), lanzamos el dashboard y salimos.
    if IS_STREAMLIT:
        build_dashboard()
        sys.exit(0)
    parser = argparse.ArgumentParser(
        description="Genera métricas rápidas para in-app-events.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent("""
            Ejemplos:
                python analytics.py                  # elegir archivo con diálogo
                python analytics.py file.csv --daily # igual que antes
        """),
    )
    parser.add_argument("csv", nargs="?", help="Ruta al CSV de AppsFlyer")
    parser.add_argument("--counts", action="store_true", help="Mostrar 'event_counts'")
    parser.add_argument("--daily", action="store_true", help="Pivot diario (total)")
    parser.add_argument("--unique-users", action="store_true", help="Pivot de usuarios únicos por día")
    parser.add_argument("--login-ratio", action="store_true", help="Ratio error-login → login OK")
    parser.add_argument("--reg-ratio", action="store_true", help="Ratio error-registro → registro OK")
    parser.add_argument("--user", metavar="ID", help="Historial de un usuario específico")

    args = parser.parse_args()
    csv_path = args.csv or pick_file()
    logging.info(f"Cargando datos desde '{csv_path}'")
    data = load(csv_path)
    logging.info(f"Datos cargados: {len(data)} filas, {len(data.columns)} columnas")

    if args.counts:
        print(event_counts(data), "\n")

    if args.daily:
        print(daily_pivot(data), "\n")

    if args.unique_users:
        print(daily_pivot(data, value="Customer User ID", agg="nunique"), "\n")

    if args.login_ratio:
        print("Login error → login OK ratio:", login_error_success_ratio(data))

    if args.reg_ratio:
        print("Registro error → registro OK ratio:", registration_error_success_ratio(data))

    if args.user:
        hist = user_history(data, args.user)
        if hist.empty:
            sys.exit(f"✗ No hay eventos para el usuario {args.user}")
        print(hist.to_string(index=False))

    if not any([args.counts, args.daily, args.unique_users, args.login_ratio, args.reg_ratio, args.user]):
        logging.info("Sin flags: entrando al menú interactivo.")
        interactive_menu(data)
    logging.info("Proceso finalizado.")

