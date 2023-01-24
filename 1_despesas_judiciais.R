# -------------------------- #
# --- DESPESAS JUDICIAIS --- #
# -------------------------- #

# bibliotecas -------------------------------------------------------------

source("R/0_bibliotecas.R")

# seleção de contas -------------------------------------------------------

contas <- readr::read_delim(
  "data/contas_judiciais.csv",
  delim = ";",
  locale = readr::locale(encoding = "UTF-8")
) |>
  dplyr::mutate(cd_cc = as.character(conta)) |>
  dplyr::select(-conta)

# despesas judiciais ------------------------------------------------------

pre_2019 <- paste0(2016:2018, " Q4")

pos_2019 <- paste0(2019:2021, " Q4")


tab_fin_jud <- purrr::map2_dfr(
  .x = c("pre-2019", "pos-2019"),
  .y = c(pre_2019, pos_2019),
  ~ arrow::read_parquet("data/tabela_financeira.parquet", as_data_frame = FALSE) |>
    dplyr::inner_join(
      contas |>
        dplyr::filter(periodo == .x),
      by = "cd_cc"
    ) |>
    dplyr::filter(ano_tri %in% .y) |>
    dplyr::group_by(ano_tri, descricao) |>
    dplyr::summarise(despesa = sum(vl_saldo_final) / 10^9) |>
    dplyr::collect()
)

writexl::write_xlsx(tab_fin_jud, "outputs/despesas_judiciais.xlsx")
