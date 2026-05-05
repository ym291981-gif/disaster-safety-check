/**
 * 安否確認: セッション集計を JSON で返す（ウェブアプリ用）
 * スプレッドシートにコンテナバインドして使用する。
 *
 * デプロイ: ウェブアプリ / 実行ユーザー: 自分 / アクセス: 全員（検証用）
 * URL 例: .../exec?sessionId=YOUR_SESSION_ID（sessionID / session_id でも可）
 */

var SHEET_EMPLOYEES = '社員マスター';
var SHEET_SESSIONS = 'セッション';
var SHEET_RESPONSES = 'レスポンス';

/** README 準拠の status 値 */
var STATUS_KEYS = ['safe', 'minor_injury', 'need_help', 'other'];

function doGet(e) {
  var p = (e && e.parameter) || {};
  /** Apps Script はクエリ名の大文字小文字を区別するため、よくある別名も受け付ける */
  var sessionId = String(p.sessionId || p.sessionID || p.session_id || '').trim();
  if (!sessionId) {
    return jsonOutput_({
      error: 'missing_sessionId',
      hint: 'Add ?sessionId=YOUR_ID (aliases: sessionID, session_id)',
    });
  }

  try {
    var payload = buildPayload_(sessionId);
    return jsonOutput_(payload);
  } catch (err) {
    return jsonOutput_({ error: 'internal_error', message: String(err) });
  }
}

function jsonOutput_(obj) {
  return ContentService.createTextOutput(JSON.stringify(obj)).setMimeType(ContentService.MimeType.JSON);
}

function buildPayload_(sessionId) {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var sessionRow = findSessionRow_(ss, sessionId);
  if (!sessionRow) {
    return { error: 'session_not_found', sessionId: sessionId };
  }

  var employees = readObjects_(getSheet_(ss, SHEET_EMPLOYEES));
  var responses = readObjects_(getSheet_(ss, SHEET_RESPONSES));
  responses = responses.filter(function (r) {
    return sessionIdFromRow_(r) === sessionId;
  });

  var empById = {};
  employees.forEach(function (row) {
    var id = String(row['社員番号'] || row['employee_id'] || '').trim();
    if (id) empById[id] = row;
  });

  var targets = employees.filter(isActiveEmployee_);
  var targetIds = targets.map(function (e) {
    return String(e['社員番号'] || e['employee_id'] || '').trim();
  }).filter(Boolean);

  var respByEmp = {};
  responses.forEach(function (r) {
    var eid = String(r['社員番号'] || r['employee_id'] || '').trim();
    if (eid) respByEmp[eid] = r;
  });

  var byStatus = { safe: 0, minor_injury: 0, need_help: 0, other: 0, unknown: 0 };
  var needHelpList = [];
  var noResponseList = [];

  targetIds.forEach(function (eid) {
    var r = respByEmp[eid];
    var emp = empById[eid] || {};
    var name = emp['氏名'] || emp['name'] || '';
    var dept = emp['部署'] || emp['department'] || '';

    if (!r) {
      noResponseList.push({ employeeId: eid, name: name, department: dept });
      return;
    }

    var st = normalizeStatus_(r['ステータス'] || r['status']);
    /** レスポンス行はあるがステータスが空＝未回答扱い（下書き行など） */
    if (!st) {
      noResponseList.push({ employeeId: eid, name: name, department: dept });
      return;
    }

    if (byStatus[st] === undefined) byStatus.unknown++;
    else byStatus[st]++;

    if (st === 'need_help') {
      needHelpList.push({
        employeeId: eid,
        name: name,
        department: dept,
        comment: r['コメント'] || r['comment'] || '',
        answeredAt: formatDate_(r['回答日時'] || r['answered_at']),
      });
    }
  });

  var answered = targetIds.length - noResponseList.length;
  var byDepartment = buildDepartmentBreakdown_(targets, respByEmp);

  return {
    sessionId: sessionId,
    session: {
      title: sessionRow['タイトル'] || sessionRow['title'] || '',
      status: sessionRow['状態'] || sessionRow['status'] || '',
      targetCountSheet: sessionRow['対象人数'] || sessionRow['target_count'] || '',
    },
    totals: {
      target: targetIds.length,
      answered: answered,
      noResponse: noResponseList.length,
      needHelp: byStatus.need_help,
    },
    byStatus: byStatus,
    needHelpList: needHelpList,
    noResponseList: noResponseList,
    byDepartment: byDepartment,
  };
}

function getSheet_(ss, name) {
  var sh = ss.getSheetByName(name);
  if (!sh) throw new Error('Sheet not found: ' + name);
  return sh;
}

function readObjects_(sheet) {
  var range = sheet.getDataRange();
  var values = range.getValues();
  if (!values.length) return [];
  var headers = values[0].map(function (h) {
    return String(h).trim();
  });
  var out = [];
  for (var i = 1; i < values.length; i++) {
    var row = values[i];
    var allEmpty = row.every(function (c) {
      return c === '' || c === null;
    });
    if (allEmpty) continue;
    var obj = {};
    for (var c = 0; c < headers.length; c++) {
      if (!headers[c]) continue;
      obj[headers[c]] = row[c];
    }
    out.push(obj);
  }
  return out;
}

/** シート1行からセッションIDを取り出す（列名の揺れを吸収） */
function sessionIdFromRow_(row) {
  return String(
    row['セッションID'] ||
      row['session_ID'] ||
      row['session_id'] ||
      row['sessionId'] ||
      ''
  ).trim();
}

function findSessionRow_(ss, sessionId) {
  var rows = readObjects_(getSheet_(ss, SHEET_SESSIONS));
  for (var i = 0; i < rows.length; i++) {
    if (sessionIdFromRow_(rows[i]) === sessionId) return rows[i];
  }
  return null;
}

function isActiveEmployee_(row) {
  var flag = row['在籍フラグ'] !== undefined && row['在籍フラグ'] !== '' ? row['在籍フラグ'] : row['active'];
  if (flag === undefined || flag === '' || flag === null) return true;
  var s = String(flag).trim().toLowerCase();
  if (s === '1' || s === 'true' || s === 'はい' || s === '在籍' || s === 'yes') return true;
  if (s === '0' || s === 'false' || s === 'いいえ' || s === '退職' || s === 'no') return false;
  return true;
}

/**
 * README の保存値（英字）に正規化する。空は ''（未回答扱いに使う）。
 * シートに日本語だけが入っている場合もここで吸収する。
 */
function normalizeStatus_(raw) {
  var s = String(raw || '').trim();
  if (!s) return '';

  var lower = s.toLowerCase();
  if (STATUS_KEYS.indexOf(lower) !== -1) return lower;

  var z = s.replace(/\s+/g, '');
  if (z === '無事') return 'safe';
  if (z === '軽症' || z === '軽傷') return 'minor_injury';
  if (z === '要救助' || z === '要救護') return 'need_help';
  if (z === 'その他') return 'other';

  return 'unknown';
}

function formatDate_(v) {
  if (v instanceof Date) return Utilities.formatDate(v, Session.getScriptTimeZone(), 'yyyy-MM-dd HH:mm:ss');
  return v ? String(v) : '';
}

function buildDepartmentBreakdown_(targets, respByEmp) {
  var map = {};
  targets.forEach(function (emp) {
    var dept = String(emp['部署'] || emp['department'] || '（未設定）').trim() || '（未設定）';
    if (!map[dept]) map[dept] = { department: dept, answered: 0, noResponse: 0 };
    var eid = String(emp['社員番号'] || emp['employee_id'] || '').trim();
    if (respByEmp[eid]) map[dept].answered++;
    else map[dept].noResponse++;
  });
  return Object.keys(map)
    .sort()
    .map(function (k) {
      return map[k];
    });
}
