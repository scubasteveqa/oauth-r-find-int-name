# Databricks Dashboard - Find by Integration Name
# =================================================
# Uses get_associations() + Filter by name to discover
# the Databricks integration GUID.
#
# Required env vars:
#   DATABRICKS_INTEGRATION_NAME - name of the Databricks integration in Connect
#   DATABRICKS_HOST             - Databricks workspace hostname
#   DATABRICKS_HTTP_PATH        - Databricks SQL warehouse HTTP path

library(shiny)
library(bslib)
library(DBI)
library(odbc)
library(connectapi)
library(plotly)
library(dplyr)

# ── Data helper ──────────────────────────────────────────────────────────────

fetch_databricks <- function(access_token) {
  conn <- dbConnect(
    odbc::odbc(),
    driver = "Databricks",
    Host = Sys.getenv("DATABRICKS_HOST"),
    Port = 443,
    HTTPPath = Sys.getenv("DATABRICKS_HTTP_PATH"),
    SSL = 1,
    ThriftTransport = 2,
    AuthMech = 11,
    Auth_Flow = 0,
    Auth_AccessToken = access_token
  )
  on.exit(dbDisconnect(conn))

  df <- dbGetQuery(conn, "
    SELECT
      t.dateTime,
      t.product,
      t.quantity,
      t.totalPrice,
      c.continent,
      c.country,
      f.name AS franchise_name
    FROM samples.bakehouse.sales_transactions t
    JOIN samples.bakehouse.sales_customers c
      ON t.customerID = c.customerID
    JOIN samples.bakehouse.sales_franchises f
      ON t.franchiseID = f.franchiseID
  ")
  names(df) <- tolower(names(df))
  df$datetime <- as.POSIXct(df$datetime)
  df$month <- format(df$datetime, "%Y-%m")
  df
}

# ── UI ────────────────────────────────────────────────────────────────────────

ui <- page_sidebar(
  title = "Databricks Analytics (find by name)",
  fillable = FALSE,
  sidebar = sidebar(
    width = 260,
    h3("Databricks Dashboard"),
    p(
      "OAuth via find by name (connectapi)",
      style = "color: #6c757d; font-size: 0.85rem; margin: 0 0 1rem 0;"
    ),
    actionButton("load_data", "Refresh Data", class = "btn-primary w-100"),
    tags$script(HTML(
      "setTimeout(function() { document.getElementById('load_data').click(); }, 500);"
    )),
    tags$hr(),
    selectInput("continent", "Continent", choices = "All", selected = "All"),
    selectInput("franchise", "Franchise", choices = "All", selected = "All")
  ),

  h4("Databricks - Bakehouse", style = "margin-top: 0.5rem;"),
  layout_columns(
    value_box("Revenue", textOutput("total_revenue"), theme = "primary"),
    value_box("Orders", textOutput("total_orders"), theme = "info"),
    value_box("Franchises", textOutput("franchise_count"), theme = "warning"),
    col_widths = c(4, 4, 4)
  ),
  layout_columns(
    card(card_header("Revenue by Franchise"), plotlyOutput("chart_franchise", height = "400px")),
    card(card_header("Revenue by Continent"), plotlyOutput("chart_continent", height = "400px")),
    col_widths = c(6, 6)
  ),
  layout_columns(
    card(card_header("Monthly Revenue Trend"), plotlyOutput("chart_trend", height = "400px")),
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
    integration_name <- Sys.getenv("DATABRICKS_INTEGRATION_NAME")
    db_assoc <- Filter(function(a) a$name == integration_name, associations)

    if (length(db_assoc) == 0) {
      showNotification(
        paste0("No Databricks integration found with name: ", integration_name),
        type = "error"
      )
      return()
    }

    db_guid <- db_assoc[[1]]$oauth_integration_guid

    tryCatch({
      creds <- connectapi::get_oauth_credentials(client, session_token, audience = db_guid)
      df <- fetch_databricks(creds$access_token)
      raw_data(df)

      updateSelectInput(session, "continent",
                        choices = c("All", sort(unique(df$continent))), selected = "All")
      updateSelectInput(session, "franchise",
                        choices = c("All", sort(unique(df$franchise_name))), selected = "All")

      showNotification(paste("Databricks:", nrow(df), "rows"), type = "message")
    }, error = function(e) {
      showNotification(paste("Databricks error:", conditionMessage(e)),
                       type = "error", duration = 10)
    })
  })

  filtered <- reactive({
    df <- raw_data()
    if (is.null(df)) return(NULL)
    if (input$continent != "All") df <- df[df$continent == input$continent, ]
    if (input$franchise != "All") df <- df[df$franchise_name == input$franchise, ]
    df
  })

  output$total_revenue <- renderText({
    df <- filtered()
    if (is.null(df)) return("--")
    paste0("$", formatC(sum(df$totalprice), format = "f", digits = 2, big.mark = ","))
  })

  output$total_orders <- renderText({
    df <- filtered()
    if (is.null(df)) return("--")
    formatC(nrow(df), format = "d", big.mark = ",")
  })

  output$franchise_count <- renderText({
    df <- filtered()
    if (is.null(df)) return("--")
    as.character(length(unique(df$franchise_name)))
  })

  output$chart_franchise <- renderPlotly({
    df <- filtered()
    if (is.null(df)) return(plot_ly() |> layout(title = "Loading..."))
    agg <- df |>
      group_by(franchise_name) |>
      summarise(revenue = sum(totalprice), .groups = "drop") |>
      arrange(revenue)
    agg$franchise_name <- factor(agg$franchise_name, levels = agg$franchise_name)
    plot_ly(agg, x = ~revenue, y = ~franchise_name, type = "bar", orientation = "h") |>
      layout(xaxis = list(title = "Revenue ($)"), yaxis = list(title = "Franchise"))
  })

  output$chart_continent <- renderPlotly({
    df <- filtered()
    if (is.null(df)) return(plot_ly() |> layout(title = "Loading..."))
    agg <- df |> group_by(continent) |> summarise(revenue = sum(totalprice), .groups = "drop")
    plot_ly(agg, labels = ~continent, values = ~revenue, type = "pie")
  })

  output$chart_trend <- renderPlotly({
    df <- filtered()
    if (is.null(df)) return(plot_ly() |> layout(title = "Loading..."))
    agg <- df |> group_by(month) |> summarise(total = sum(totalprice), .groups = "drop") |> arrange(month)
    plot_ly(agg, x = ~month, y = ~total, type = "scatter", mode = "lines+markers") |>
      layout(xaxis = list(title = "Month"), yaxis = list(title = "Revenue ($)"))
  })
}

shinyApp(ui, server)
