const DEFAULT_MAX_ROWS = 50000;
const DATA_PAGE_NAME = "Data";

function doPost(e) {
  const lock = LockService.getScriptLock();
  lock.waitLock(20000);

  try {
    const payload = JSON.parse(e.postData.contents);
    assertSecret_(payload.secret);

    const book = String(payload.book || "");
    const headers = payload.headers || [];
    const rows = payload.rows || [];
    const maxRows = Number(payload.max_rows || DEFAULT_MAX_ROWS);

    if (!book) {
      throw new Error("Missing book");
    }
    if (!headers.length) {
      throw new Error("Missing headers");
    }
    if (!Array.isArray(rows)) {
      throw new Error("Rows must be an array");
    }

    const spreadsheetId = getSpreadsheetId_(book);
    const ss = SpreadsheetApp.openById(spreadsheetId);
    const page = ensurePage_(ss, DATA_PAGE_NAME);

    ensureHeaders_(page, headers);
    if (rows.length > 0) {
      page.getRange(page.getLastRow() + 1, 1, rows.length, headers.length).setValues(rows);
      trimPage_(page, maxRows);
    }

    return json_({
      ok: true,
      book,
      spreadsheet_id: spreadsheetId,
      rows_received: rows.length,
      page_rows: page.getLastRow(),
    });
  } catch (err) {
    return json_({ ok: false, error: String(err) });
  } finally {
    lock.releaseLock();
  }
}

function setupAllBooks() {
  const books = [
    "Hyper_BRENTOIL",
    "Hyper_GOLD",
    "Hyper_SILVER",
    "Hyper_WTI",
    "Lighter_BRENTOIL",
    "Lighter_GOLD",
    "Lighter_SILVER",
    "Lighter_WTI",
  ];

  books.forEach((book) => {
    const ss = SpreadsheetApp.openById(getSpreadsheetId_(book));
    ensurePage_(ss, DATA_PAGE_NAME);
  });
}

function getSpreadsheetId_(book) {
  const key = `SPREADSHEET_ID_${book}`;
  const spreadsheetId = PropertiesService.getScriptProperties().getProperty(key);
  if (!spreadsheetId) {
    throw new Error(`Missing script property ${key}`);
  }
  return spreadsheetId;
}

function ensurePage_(ss, name) {
  let page = ss.getSheetByName(name);
  if (!page) {
    page = ss.insertSheet(name);
  }
  return page;
}

function ensureHeaders_(page, headers) {
  if (page.getLastRow() === 0) {
    page.getRange(1, 1, 1, headers.length).setValues([headers]);
    page.setFrozenRows(1);
    return;
  }

  const current = page.getRange(1, 1, 1, headers.length).getValues()[0];
  const matches = headers.every((value, index) => current[index] === value);
  if (!matches) {
    throw new Error(`Header mismatch on ${page.getParent().getName()} / ${page.getName()}`);
  }
}

function trimPage_(page, maxRows) {
  if (!maxRows || maxRows <= 0) {
    return;
  }

  const lastRow = page.getLastRow();
  const excess = lastRow - maxRows;
  if (excess > 0) {
    page.deleteRows(2, excess);
  }
}

function assertSecret_(incomingSecret) {
  const expected = PropertiesService.getScriptProperties().getProperty("INGEST_SECRET");
  if (!expected) {
    throw new Error("Set script property INGEST_SECRET before deploying");
  }
  if (incomingSecret !== expected) {
    throw new Error("Invalid secret");
  }
}

function json_(obj) {
  return ContentService
    .createTextOutput(JSON.stringify(obj))
    .setMimeType(ContentService.MimeType.JSON);
}
