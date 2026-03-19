// ═══════════════════════════════════════════════════════════
// Brand Intelligence — Looker Studio Community Connector
// ═══════════════════════════════════════════════════════════
// Paste this entire file into script.google.com as Code.gs
// Then deploy as: Deploy → New Deployment → Add-on
// ═══════════════════════════════════════════════════════════

var SHEET_ID = "YOUR_GOOGLE_SHEET_ID_HERE";

// ── Connector info shown in Looker Studio ──
function getAuthType() {
  return { type: "NONE" };
}

function getConfig(request) {
  var config = DataStudioApp.createCommunityConnector().getConfig();

  config.newInfo()
    .setId("info")
    .setText("Reads brand + generic trend data from your Google Sheet. Make sure fetcher_to_sheets.py has been run at least once.");

  config.newSelectSingle()
    .setId("tabName")
    .setName("Country / Market")
    .setHelpText("Select which market to load.")
    .setAllowOverride(true)
    .addOption(config.newOptionBuilder().setLabel("🇨🇦 Canada — Ontario").setValue("CA-Ontario"))
    .addOption(config.newOptionBuilder().setLabel("🇨🇦 Canada — All").setValue("CA-All"))
    .addOption(config.newOptionBuilder().setLabel("🇬🇷 Greece").setValue("Greece"))
    .addOption(config.newOptionBuilder().setLabel("🇲🇽 Mexico").setValue("Mexico"))
    .addOption(config.newOptionBuilder().setLabel("🇸🇪 Sweden").setValue("Sweden"))
    .addOption(config.newOptionBuilder().setLabel("🇩🇰 Denmark").setValue("Denmark"))
    .addOption(config.newOptionBuilder().setLabel("🇪🇸 Spain").setValue("Spain"))
    .addOption(config.newOptionBuilder().setLabel("🇷🇴 Romania").setValue("Romania"))
    .addOption(config.newOptionBuilder().setLabel("🇮🇹 Italy").setValue("Italy"));

  config.newSelectSingle()
    .setId("dataType")
    .setName("Data Type")
    .setHelpText("Show brands, generic trends, or both.")
    .setAllowOverride(true)
    .addOption(config.newOptionBuilder().setLabel("All (brands + generic)").setValue("all"))
    .addOption(config.newOptionBuilder().setLabel("Brands only").setValue("brand"))
    .addOption(config.newOptionBuilder().setLabel("Generic trends only").setValue("generic"));

  config.setDateRangeRequired(true);
  return config.build();
}

// ── Schema: all available fields ──
function getFields() {
  var fields = DataStudioApp.createCommunityConnector().getFields();
  var types  = DataStudioApp.createCommunityConnector().FieldType;
  var aggs   = DataStudioApp.createCommunityConnector().AggregationType;

  fields.newDimension()
    .setId("date").setName("Date")
    .setType(types.YEAR_MONTH_DAY);

  fields.newDimension()
    .setId("keyword").setName("Brand / Keyword")
    .setType(types.TEXT);

  fields.newDimension()
    .setId("type").setName("Type")
    .setType(types.TEXT)
    .setDescription("'brand' or 'generic'");

  fields.newDimension()
    .setId("country").setName("Country")
    .setType(types.TEXT);

  fields.newDimension()
    .setId("region").setName("Region")
    .setType(types.TEXT);

  fields.newDimension()
    .setId("monthYear").setName("Month")
    .setType(types.TEXT);

  fields.newDimension()
    .setId("year").setName("Year")
    .setType(types.TEXT);

  fields.newDimension()
    .setId("source").setName("Data Source")
    .setType(types.TEXT)
    .setDescription("pytrends or serpapi");

  fields.newDimension()
    .setId("fetchedAt").setName("Last Fetched")
    .setType(types.TEXT);

  fields.newMetric()
    .setId("interest").setName("Search Interest (0-100)")
    .setType(types.NUMBER)
    .setAggregation(aggs.AVG);

  fields.newMetric()
    .setId("interestMax").setName("Peak Interest")
    .setType(types.NUMBER)
    .setAggregation(aggs.MAX);

  fields.newMetric()
    .setId("interestMin").setName("Min Interest")
    .setType(types.NUMBER)
    .setAggregation(aggs.MIN);

  return fields;
}

function getSchema(request) {
  return { schema: getFields().build() };
}

// ── Data fetch ──
function getData(request) {
  var tabName  = request.configParams.tabName  || "CA-Ontario";
  var dataType = request.configParams.dataType || "all";

  var ss    = SpreadsheetApp.openById(SHEET_ID);
  var sheet = ss.getSheetByName(tabName);

  if (!sheet) {
    DataStudioApp.createCommunityConnector()
      .newUserError()
      .setText("Tab '" + tabName + "' not found. Run fetcher_to_sheets.py first.")
      .throwException();
  }

  var raw = sheet.getDataRange().getValues();
  if (raw.length < 2) {
    DataStudioApp.createCommunityConnector()
      .newUserError()
      .setText("No data in '" + tabName + "'. Run fetcher_to_sheets.py to populate it.")
      .throwException();
  }

  // Map headers to column indices
  var headers = raw[0].map(function(h) { return String(h).toLowerCase().trim(); });
  var col = {
    date:      headers.indexOf("date"),
    keyword:   headers.indexOf("keyword"),
    interest:  headers.indexOf("interest"),
    type:      headers.indexOf("type"),
    country:   headers.indexOf("country"),
    region:    headers.indexOf("region"),
    monthYear: headers.indexOf("monthyear"),
    year:      headers.indexOf("year"),
    source:    headers.indexOf("source"),
    fetchedAt: headers.indexOf("fetchedat"),
  };

  var requestedFields = request.fields.map(function(f) { return f.name; });

  var startDate = request.dateRange ? request.dateRange.startDate : null;
  var endDate   = request.dateRange ? request.dateRange.endDate   : null;

  var rows = [];

  for (var i = 1; i < raw.length; i++) {
    var row = raw[i];
    if (!row[col.date]) continue;

    // Filter by data type (brand / generic / all)
    var rowType = String(row[col.type] || "").toLowerCase();
    if (dataType !== "all" && rowType !== dataType) continue;

    // Filter by date range
    var dateStr = String(row[col.date]).replace(/-/g, "");
    if (startDate && dateStr < startDate) continue;
    if (endDate   && dateStr > endDate)   continue;

    var values = [];
    requestedFields.forEach(function(fieldId) {
      switch(fieldId) {
        case "date":      values.push(dateStr); break;
        case "keyword":   values.push(String(row[col.keyword]   || "")); break;
        case "type":      values.push(String(row[col.type]      || "")); break;
        case "country":   values.push(String(row[col.country]   || "")); break;
        case "region":    values.push(String(row[col.region]    || "")); break;
        case "monthYear": values.push(String(row[col.monthYear] || "")); break;
        case "year":      values.push(String(row[col.year]      || "")); break;
        case "source":    values.push(String(row[col.source]    || "")); break;
        case "fetchedAt": values.push(String(row[col.fetchedAt] || "")); break;
        case "interest":
        case "interestMax":
        case "interestMin":
          values.push(Number(row[col.interest]) || 0); break;
        default: values.push(null);
      }
    });

    rows.push({ values: values });
  }

  var schema = getFields().asArray().filter(function(f) {
    return requestedFields.indexOf(f.getId()) !== -1;
  });

  return { schema: schema, rows: rows, filtersApplied: false };
}

function isAdminUser()  { return false; }
function isAuthValid()  { return true; }
