# Snowflake Dashboard - Find by Integration Name
# ================================================
# Uses get_associations() + Filter by name to discover
# the Snowflake integration GUID.
#
# Required env vars:
#   SNOWFLAKE_INTEGRATION_NAME  - name of the Snowflake integration in Connect
#   SNOWFLAKE_WAREHOUSE         - Snowflake warehouse
#   SNOWFLAKE_DATABASE          - Snowflake database
#   SNOWFLAKE_SCHEMA            - Snowflake schema

library(shiny)
library(bslib)
library(DBI)
library(odbc)
library(connectapi)
library(plotly)
library(dplyr)

# ── Data helper ──────────────────────────────────────────────────────────────

fetch_snowflake <- function(access_token) {
  conn <- dbConnect(
    odbc::odbc(),
    driver = "Snowflake",
    Server = paste0(Sys.getenv("SNOWFLAKE_ACCOUNT"), ".snowflakecomputing.com"),
    Database = Sys.getenv("SNOWFLAKE_DATABASE"),
    Schema = Sys.getenv("SNOWFLAKE_SCHEMA"),
    Warehouse = Sys.getenv("SNOWFLAKE_WAREHOUSE"),
    Authenticator = "oauth",
    Token = access_token
  )
  on.exit(dbDisconnect(conn))

  df <- dbGetQuery(conn, "SELECT * FROM SALES")
  names(df) <- toupper(names(df))
  df$SALE_DATE <- as.Date(df$SALE_DATE)
  df$MONTH <- format(df$SALE_DATE, "%Y-%m")
  df
}

# ── UI ────────────────────────────────────────────────────────────────────────

ui <- page_sidebar(
  title = "Snowflake Analytics (find by name)",
  fillable = FALSE,
  sidebar = sidebar(
    width = 260,
    h3("Snowflake Dashboard"),
    p(
      "OAuth via find by name (connectapi)",
      style = "color: #6c757d; font-size: 0.85rem; margin: 0 0 1rem 0;"
    ),
    actionButton("load_data", "Refresh Data", class = "btn-primary w-100"),
    tags$script(HTML(
      "setTimeout(function() { document.getElementById('load_data').click(); }, 500);"
    )),
    tags$hr(),
    selectInput("category", "Category", choices = "All", selected = "All"),
    selectInput("region", "Region", choices = "All", selected = "All")
  ),

  h4("Snowflake - Sales", style = "margin-top: 0.5rem;"),
  layout_columns(
    value_box("Total Sales", textOutput("total_sales"), theme = "primary"),
    value_box("Orders", textOutput("total_orders"), theme = "info"),
    value_box("Avg Order", textOutput("avg_order"), theme = "success"),
    col_widths = c(4, 4, 4)
  ),
  layout_columns(
    card(card_header("Sales by Category"), plotlyOutput("chart_category", height = "400px")),
    card(card_header("Sales by Region"), plotlyOutput("chart_region", height = "400px")),
    col_widths = c(6, 6)
  ),
  layout_columns(
    card(card_header("Monthly Sales Trend"), plotlyOutput("chart_trend", height = "400px")),
    col_widths = 12
  )
)

# ── Server ────────────────────────────────────────────────────────────────────

server <- function(input, output, session) {
  raw_data <- reactiveVal(NULL)

  observeEvent(input$load_data, {
    session_token <- session$request$HTTP_POSIT_CONNECT_USER_SESSION_TOKEN
    if (is.null(session_token) || session_token == "") {
      showNotification("No session token found. Deploy this app on Posit Connect.",
                       type = "error")
      return()
    }

    client <- connectapi::connect()
    current_content <- connectapi::content_item(client, Sys.getenv("CONNECT_CONTENT_GUID"))
    associations <- connectapi::get_associations(current_content)

    # Discover integration by name
    integration_name <- Sys.getenv("SNOWFLAKE_INTEGRATION_NAME")
    sf_assoc <- Filter(function(a) a$oauth_integration_name == integration_name, associations) 

    if (length(sf_assoc) == 0) {
      showNotification(
        paste0("No Snowflake integration found with name: ", integration_name),
        type = "error"
      )
      return()
    }

    sf_guid <- sf_assoc[[1]]$oauth_integration_guid

    tryCatch({
      creds <- connectapi::get_oauth_credentials(client, session_token, audience = sf_guid)
      df <- fetch_snowflake(creds$access_token)
      raw_data(df)

      updateSelectInput(session, "category",
                        choices = c("All", sort(unique(df$CATEGORY))), selected = "All")
      updateSelectInput(session, "region",
                        choices = c("All", sort(unique(df$REGION))), selected = "All")

      showNotification(paste("Snowflake:", nrow(df), "rows"), type = "message")
    }, error = function(e) {
      showNotification(paste("Snowflake error:", conditionMessage(e)),
                       type = "error", duration = 10)
    })
  })

  filtered <- reactive({
    df <- raw_data()
    if (is.null(df)) return(NULL)
    if (input$category != "All") df <- df[df$CATEGORY == input$category, ]
    if (input$region != "All") df <- df[df$REGION == input$region, ]
    df
  })

  output$total_sales <- renderText({
    df <- filtered()
    if (is.null(df)) return("--")
    paste0("$", formatC(sum(df$TOTAL_AMOUNT), format = "f", digits = 2, big.mark = ","))
  })

  output$total_orders <- renderText({
    df <- filtered()
    if (is.null(df)) return("--")
    formatC(nrow(df), format = "d", big.mark = ",")
  })

  output$avg_order <- renderText({
    df <- filtered()
    if (is.null(df) || nrow(df) == 0) return("--")
    paste0("$", formatC(mean(df$TOTAL_AMOUNT), format = "f", digits = 2, big.mark = ","))
  })

  output$chart_category <- renderPlotly({
    df <- filtered()
    if (is.null(df)) return(plot_ly() |> layout(title = "Loading..."))
    agg <- df |> group_by(CATEGORY) |> summarise(total = sum(TOTAL_AMOUNT), .groups = "drop")
    plot_ly(agg, x = ~CATEGORY, y = ~total, color = ~CATEGORY, type = "bar") |>
      layout(xaxis = list(title = "Category"), yaxis = list(title = "Sales ($)"))
  })

  output$chart_region <- renderPlotly({
    df <- filtered()
    if (is.null(df)) return(plot_ly() |> layout(title = "Loading..."))
    agg <- df |> group_by(REGION) |> summarise(total = sum(TOTAL_AMOUNT), .groups = "drop")
    plot_ly(agg, labels = ~REGION, values = ~total, type = "pie")
  })

  output$chart_trend <- renderPlotly({
    df <- filtered()
    if (is.null(df)) return(plot_ly() |> layout(title = "Loading..."))
    agg <- df |> group_by(MONTH) |> summarise(total = sum(TOTAL_AMOUNT), .groups = "drop") |> arrange(MONTH)
    plot_ly(agg, x = ~MONTH, y = ~total, type = "scatter", mode = "lines+markers") |>
      layout(xaxis = list(title = "Month"), yaxis = list(title = "Sales ($)"))
  })
}

shinyApp(ui, server)
