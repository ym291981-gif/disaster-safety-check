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
/** 本人確認用トークンを保存するタブ名（手順どおりに新規作成する） */
var SHEET_TOKENS = '回答トークン';
/**
 * true: トークンで1回送信すると「使用済み」になり再送信不可（本番向け）。
 * false: 検証しやすいよう何度でも送信できる（課題・動作確認向け）。
 */
var TOKEN_MARK_USED_AFTER_SAVE = false;

/** README 準拠の status 値 */
var STATUS_KEYS = ['safe', 'minor_injury', 'need_help', 'other'];

function doGet(e) {
  var p = (e && e.parameter) || {};
  var token = String(p.token || '').trim();
  var mode = String(p.mode || '').trim();

  /** 回答ページ用: トークンが有効か・どのセッションかだけ返す（社員番号は返さない） */
  if (token && mode === 'tokenInfo') {
    try {
      return jsonOutput_(getTokenInfo_(token));
    } catch (err) {
      return jsonOutput_({ ok: false, error: String(err.message || err) });
    }
  }

  /** Apps Script はクエリ名の大文字小文字を区別するため、よくある別名も受け付ける */
  var sessionId = String(p.sessionId || p.sessionID || p.session_id || '').trim();
  if (!sessionId) {
    return jsonOutput_({
      error: 'missing_sessionId',
      hint: '集計 JSON: ?sessionId=... / トークン確認: ?token=...&mode=tokenInfo',
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

/**
 * 回答の保存（フォーム POST または JSON POST）
 * フォームの name 例: session_id, employee_id, status, comment, response_channel
 */
function doPost(e) {
  try {
    var data = parsePostPayload_(e);
    var out = saveResponse_(data);
    return jsonOutput_(out);
  } catch (err) {
    return jsonOutput_({ ok: false, error: String(err.message || err) });
  }
}

function parsePostPayload_(e) {
  if (!e) return {};
  var ct = (e.postData && e.postData.type) || '';
  if (e.postData && e.postData.contents && ct.indexOf('application/json') !== -1) {
    try {
      return JSON.parse(e.postData.contents);
    } catch (x) {
      return {};
    }
  }
  return e.parameter || {};
}

function headerIndex_(headers, candidates) {
  for (var c = 0; c < candidates.length; c++) {
    var name = candidates[c];
    var j = headers.indexOf(name);
    if (j !== -1) return j;
  }
  return -1;
}

function isTruthyUsed_(v) {
  if (v === '' || v === null || v === undefined) return false;
  var s = String(v).trim().toLowerCase();
  return s === '1' || s === 'true' || s === 'はい' || s === 'yes';
}

function isExpiredToken_(v) {
  if (!v || v === '') return false;
  if (Object.prototype.toString.call(v) === '[object Date]' && !isNaN(v.getTime())) {
    return new Date() > v;
  }
  var d = new Date(v);
  if (isNaN(d.getTime())) return false;
  return new Date() > d;
}

/**
 * トークン列で一致する行を探す。見つからなければ null。
 * @returns {{ sheetRow: number, sessionId: string, empId: string, usedRaw: *, expiresRaw: *, meta: Object }}
 */
function findTokenMatch_(ss, tokenStr) {
  var sh = ss.getSheetByName(SHEET_TOKENS);
  if (!sh) return null;
  var values = sh.getDataRange().getValues();
  if (values.length < 2) return null;
  var headers = values[0].map(function (h) {
    return String(h).trim();
  });
  var cTok = headerIndex_(headers, ['token', 'トークン']);
  var cSid = headerIndex_(headers, ['session_id', 'セッションID', 'session_ID', 'sessionId']);
  var cEmp = headerIndex_(headers, ['社員番号', 'employee_id']);
  var cUsed = headerIndex_(headers, ['used', '使用済み']);
  var cExp = headerIndex_(headers, ['expires_at', '有効期限']);
  if (cTok < 0 || cSid < 0 || cEmp < 0) return null;

  for (var r = 1; r < values.length; r++) {
    var row = values[r];
    if (String(row[cTok] || '').trim() === tokenStr) {
      return {
        sheetRow: r + 1,
        sessionId: String(row[cSid] || '').trim(),
        empId: String(row[cEmp] || '').trim(),
        usedRaw: cUsed >= 0 ? row[cUsed] : '',
        expiresRaw: cExp >= 0 ? row[cExp] : '',
        sh: sh,
        cUsed: cUsed,
      };
    }
  }
  return null;
}

function validateTokenForSave_(ss, tokenStr) {
  var m = findTokenMatch_(ss, tokenStr);
  if (!m) throw new Error('トークンが無効です（URL を確認するか、管理者に再発行を依頼してください）');
  if (!m.sessionId || !m.empId) throw new Error('トークンデータが不完全です');
  if (isTruthyUsed_(m.usedRaw)) throw new Error('この回答用リンクはすでに使用済みです');
  if (isExpiredToken_(m.expiresRaw)) throw new Error('この回答用リンクの有効期限が切れています');
  return m;
}

function markTokenUsed_(ss, tokenStr) {
  if (!TOKEN_MARK_USED_AFTER_SAVE) return;
  var m = findTokenMatch_(ss, tokenStr);
  if (!m || m.cUsed < 0) return;
  m.sh.getRange(m.sheetRow, m.cUsed + 1).setValue(true);
}

/** 回答ページがセッション名だけ表示するための軽い GET API */
function getTokenInfo_(tokenStr) {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var m = findTokenMatch_(ss, tokenStr);
  if (!m) return { ok: false, error: 'invalid_token' };
  if (isTruthyUsed_(m.usedRaw)) return { ok: false, error: 'token_already_used' };
  if (isExpiredToken_(m.expiresRaw)) return { ok: false, error: 'token_expired' };
  var sessionRow = findSessionRow_(ss, m.sessionId);
  var title = sessionRow ? String(sessionRow['タイトル'] || sessionRow['title'] || '') : '';
  return { ok: true, session_id: m.sessionId, session_title: title };
}

function deleteTokenRowsForSession_(ss, sessionIdStr) {
  var sh = ss.getSheetByName(SHEET_TOKENS);
  if (!sh) return;
  var values = sh.getDataRange().getValues();
  if (values.length < 2) return;
  var headers = values[0].map(function (h) {
    return String(h).trim();
  });
  var cSid = headerIndex_(headers, ['session_id', 'セッションID', 'session_ID', 'sessionId']);
  if (cSid < 0) return;
  for (var r = values.length; r >= 2; r--) {
    if (String(values[r - 1][cSid] || '').trim() === sessionIdStr) {
      sh.deleteRow(r);
    }
  }
}

function appendTokenRow_(sh, headers, token, sessionIdStr, empId) {
  var h = headers.map(function (x) {
    return String(x).trim();
  });
  var row = [];
  for (var i = 0; i < h.length; i++) row.push('');
  var set = function (names, val) {
    var idx = headerIndex_(h, names);
    if (idx >= 0) row[idx] = val;
  };
  set(['token', 'トークン'], token);
  set(['session_id', 'セッションID', 'session_ID', 'sessionId'], sessionIdStr);
  set(['社員番号', 'employee_id'], empId);
  set(['used', '使用済み'], false);
  sh.appendRow(row);
}

/**
 * 管理者がスクリプト編集画面から実行する: 指定セッションのトークンを全員分まき直す。
 * @param {string} sessionIdStr セッション ID（セッションシートと同じ文字列）
 * @returns {Object[]} { employee_id, token } の配列（ログ・リンク組み立て用）
 */
function issueTokensForSession(sessionIdStr) {
  sessionIdStr = String(sessionIdStr || '').trim();
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  if (!sessionIdStr) throw new Error('セッション ID を渡してください（例: issueTokensForSession(\'202605\')）');
  if (!findSessionRow_(ss, sessionIdStr)) throw new Error('セッションが見つかりません: ' + sessionIdStr);

  var sh = ss.getSheetByName(SHEET_TOKENS);
  if (!sh) throw new Error('シート「' + SHEET_TOKENS + '」がありません。スプレッドシートに作成してください。');

  var vr = sh.getDataRange().getValues();
  if (!vr.length) throw new Error('回答トークンシートの1行目にヘッダーを入れてください');
  var headers = vr[0].map(function (cell) {
    return String(cell).trim();
  });

  deleteTokenRowsForSession_(ss, sessionIdStr);

  var targets = readObjects_(getSheet_(ss, SHEET_EMPLOYEES)).filter(isActiveEmployee_);
  var issued = [];
  targets.forEach(function (emp) {
    var empId = String(emp['社員番号'] || emp['employee_id'] || '').trim();
    if (!empId) return;
    var tok =
      Utilities.getUuid().replace(/-/g, '') + Utilities.getUuid().replace(/-/g, '').substring(0, 12);
    appendTokenRow_(sh, headers, tok, sessionIdStr, empId);
    issued.push({ employee_id: empId, token: tok });
  });
  return issued;
}

/**
 * issueTokensForSession を「実行ボタンだけ」で動かすための入口です。
 *
 * issueTokensForSession 本体は「どのセッション用か」を引数で渡す設計ですが、
 * Apps Script のエディタ上部の ▶ 実行では、通常その引数を入力できません。
 * そのため、セッション ID を一度ここに書いてから、この関数を実行します。
 *
 * 手順（短く）:
 * 1. SESSION_ID_FOR_TOKEN_ISSUE をセッションシートと同じ ID に書き換える
 * 2. 保存（Ctrl+S）
 * 3. 関数プルダウンで runIssueTokensFromEditor を選ぶ → ▶ 実行
 */
var SESSION_ID_FOR_TOKEN_ISSUE = '202605';

function runIssueTokensFromEditor() {
  issueTokensForSession(SESSION_ID_FOR_TOKEN_ISSUE);
}

function ensureEmployeeExists_(ss, empId) {
  var rows = readObjects_(getSheet_(ss, SHEET_EMPLOYEES));
  for (var i = 0; i < rows.length; i++) {
    var id = String(rows[i]['社員番号'] || rows[i]['employee_id'] || '').trim();
    if (id === empId) return;
  }
  throw new Error('社員番号が社員マスターに存在しません');
}

/**
 * レスポンスシートへ 1 行追加、または同一 session + 社員番号の行を上書き
 */
function upsertResponseRow_(ss, sessionId, empId, statusEng, comment, channel) {
  var sh = getSheet_(ss, SHEET_RESPONSES);
  var range = sh.getDataRange();
  var values = range.getValues();
  if (!values.length) throw new Error('レスポンスシートが空です。1行目にヘッダーを入れてください');
  var headers = values[0].map(function (h) {
    return String(h).trim();
  });

  var cS = headerIndex_(headers, ['session_id', 'セッションID', 'session_ID', 'sessionId']);
  var cE = headerIndex_(headers, ['社員番号', 'employee_id']);
  var cSt = headerIndex_(headers, ['ステータス', 'status']);
  var cCo = headerIndex_(headers, ['コメント', 'comment']);
  var cAns = headerIndex_(headers, ['回答日時', 'answered_at']);
  var cUpd = headerIndex_(headers, ['更新日時', 'updated_at']);
  var cCh = headerIndex_(headers, ['回答チャネル', 'response_channel']);

  if (cS < 0 || cE < 0 || cSt < 0) {
    throw new Error('レスポンスシートに session_id 系・社員番号・ステータス列がありません');
  }

  var now = new Date();
  var numCols = headers.length;
  var sheetRow = -1;
  for (var r = 1; r < values.length; r++) {
    var row = values[r];
    if (String(row[cS] || '').trim() === sessionId && String(row[cE] || '').trim() === empId) {
      sheetRow = r + 1;
      break;
    }
  }

  if (sheetRow > 0) {
    sh.getRange(sheetRow, cSt + 1).setValue(statusEng);
    if (cCo >= 0) sh.getRange(sheetRow, cCo + 1).setValue(comment);
    if (cAns >= 0) sh.getRange(sheetRow, cAns + 1).setValue(now);
    if (cUpd >= 0) sh.getRange(sheetRow, cUpd + 1).setValue(now);
    if (cCh >= 0) sh.getRange(sheetRow, cCh + 1).setValue(channel);
  } else {
    var newRow = [];
    for (var c = 0; c < numCols; c++) newRow.push('');
    newRow[cS] = sessionId;
    newRow[cE] = empId;
    newRow[cSt] = statusEng;
    if (cCo >= 0) newRow[cCo] = comment;
    if (cAns >= 0) newRow[cAns] = now;
    if (cUpd >= 0) newRow[cUpd] = now;
    if (cCh >= 0) newRow[cCh] = channel;
    sh.appendRow(newRow);
  }
}

function saveResponse_(data) {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var token = String(data.token || '').trim();
  var sessionId;
  var empId;

  if (token) {
    var tm = validateTokenForSave_(ss, token);
    sessionId = tm.sessionId;
    empId = tm.empId;
  } else {
    sessionId = String(data.session_id || data.sessionId || '').trim();
    empId = String(data.employee_id || data.employeeId || data.社員番号 || '').trim();
  }

  var statusRaw = data.status || data.ステータス || '';
  var comment = String(data.comment || data.コメント || '').trim();
  var channel = String(data.response_channel || data.responseChannel || 'web').trim();

  if (!sessionId || !empId) {
    throw new Error('session_id と employee_id が必要です（またはフォームに token を含めてください）');
  }
  if (!findSessionRow_(ss, sessionId)) {
    throw new Error('セッションが見つかりません（ID をセッションシートと合わせてください）');
  }

  var st = normalizeStatus_(statusRaw);
  if (!st || st === 'unknown') {
    throw new Error(
      'ステータスが不正です。safe / minor_injury / need_help / other、または 無事・軽症・要救助・その他 を送ってください'
    );
  }
  if (st === 'other' && !comment) {
    throw new Error('その他を選ぶ場合はコメントを入力してください');
  }

  ensureEmployeeExists_(ss, empId);
  upsertResponseRow_(ss, sessionId, empId, st, comment, channel);

  if (token) markTokenUsed_(ss, token);

  return { ok: true, session_id: sessionId, employee_id: empId, status: st, via_token: !!token };
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
