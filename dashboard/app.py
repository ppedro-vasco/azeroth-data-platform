import streamlit as st
import pandas as pd
import requests
import os

# Configuração da Página (Layout Wide para caber a tabela)
st.set_page_config(page_title="Azeroth Data Platform", page_icon="⚔️", layout="wide")

# URL da API
API_URL = os.getenv("API_URL", "http://api:8000")

# --- FUNÇÕES AUXILIARES ---
def get_opportunities(filter_type="BUY"):
    """Busca oportunidades na API e retorna um DataFrame"""
    try:
        response = requests.get(f"{API_URL}/analytics/opportunities", params={"recommendation": filter_type})
        if response.status_code == 200:
            data = response.json()
            return pd.DataFrame(data)
        else:
            st.error(f"Erro na API: {response.status_code}")
            return pd.DataFrame()
    except Exception as e:
        st.error(f"Erro de conexão: {e}")
        return pd.DataFrame()

def format_gold(copper_value):
    """Converte cobre para string formatada de ouro (ex: 10000 -> 1g)"""
    if pd.isna(copper_value):
        return "0g"
    return f"{int(copper_value / 10000):,}g".replace(",", ".")

# --- INTERFACE ---

st.title("⚔️ Azeroth Data Platform")
st.markdown("### Inteligência de Mercado via Engenharia de Dados")

# Barra Lateral
st.sidebar.header("Filtros")
rec_filter = st.sidebar.radio("Recomendação:", ["BUY", "SELL"], index=0)

if st.sidebar.button("🔄 Atualizar Dados"):
    st.cache_data.clear() # Limpa o cache para forçar nova requisição

# Lógica Principal
st.divider()

st.subheader(f"Oportunidades Identificadas: {rec_filter}")

# 1. Buscar Dados
df = get_opportunities(rec_filter)

if not df.empty:
    # 2. Tratamento de Dados para Exibição
    # Criamos uma cópia para não alterar os dados brutos se precisarmos depois
    display_df = df.copy()
    
    # Formatando Preços (Cobre -> Ouro)
    display_df["Preço Atual"] = display_df["current_price"].apply(format_gold)
    display_df["Média (7d)"] = display_df["avg_price_7d"].apply(format_gold)
    
    # Selecionar e Renomear Colunas
    display_df = display_df[[
        "item_name", 
        "Preço Atual", 
        "Média (7d)", 
        "z_score", 
        "snapshot_date"
    ]].rename(columns={
        "item_name": "Item",
        "z_score": "Desvio (Z-Score)",
        "snapshot_date": "Data da Análise"
    })

    # 3. Exibir Tabela Interativa
    st.dataframe(
        display_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Item": st.column_config.TextColumn("Nome do Item", help="Nome do item no jogo"),
            "Desvio (Z-Score)": st.column_config.ProgressColumn(
                "Potencial (Z-Score)",
                format="%.2fσ",
                min_value=-5,
                max_value=5,
                help="Quanto maior a barra, maior o desvio do preço normal."
            ),
        }
    )
    
    # Métricas Rápidas (KPIs)
    col1, col2, col3 = st.columns(3)
    col1.metric("Total de Oportunidades", len(df))
    # Exemplo: Pega o item com maior desconto (menor z-score)
    best_opp = df.loc[df['z_score'].idxmin()]
    col2.metric("Melhor Oportunidade", best_opp['item_name'])
    col3.metric("Potencial", f"{best_opp['z_score']:.2f}σ")
    
else:
    st.warning("Nenhuma oportunidade encontrada com os filtros atuais (ou falha na conexão).")