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
/** 管理者の「発信」履歴を残す（無ければ自動作成） */
var SHEET_BROADCASTS = '発信ログ';
/**
 * 配布対象キュー（メール/LINE 送信の出発点）。送信処理はこのシートを読む。
 *
 * 送信方針（無料枠・手動再送）:
 * - メール・LINE とも「無料枠の範囲内」だけ送る。残量不足や上限エラーが出たらそれ以上送らず処理を止める（有料枠へは進めない）。
 * - 失敗した行は自動再送しない（上限の浪費・ループを避ける）。キューを failed 等にし、手動で確認・再送する。
 * 実装側は OUTBOX_SEND_ONLY_WITHIN_FREE_TIER / OUTBOX_NO_AUTO_RETRY を参照すること。
 */
var SHEET_OUTBOX = '配布キュー';
/** 複数管理者で共有する「担当・対応チェック」 */
var SHEET_ASSIGNMENTS = '対応アサイン';
/** LINE Webhook 受信ログ（userId 取得・紐付け用） */
var SHEET_LINE_WEBHOOK_LOG = 'LINE連携ログ';
/**
 * true: トークンで1回送信すると「使用済み」になり再送信不可（本番向け）。
 * false: 検証しやすいよう何度でも送信できる（課題・動作確認向け）。
 */
var TOKEN_MARK_USED_AFTER_SAVE = false;

/**
 * true: メール/LINE は無料枠に収まる分だけ送る。事前の残量チェックや API の上限応答で「これ以上送れない」と分かったら打ち切る。
 * false にした場合のみ、枠外まで送る実装が許容される（本プロジェクトでは想定しない）。
 */
var OUTBOX_SEND_ONLY_WITHIN_FREE_TIER = true;

/**
 * true: 送信失敗時にキューを自動リトライしない。管理者が内容・上限を確認してから手動で送り直す。
 */
var OUTBOX_NO_AUTO_RETRY = true;

/**
 * 配布キューからのメール一括送信: 1回の実行で処理する最大行数（queued のみ）。
 * Gmail の無料枠・6分実行上限を考慮し控えめにしておく。
 */
var OUTBOX_EMAIL_MAX_PER_RUN = 50;

/**
 * スクリプトタイムゾーンの「同一日」に送れる通数の上限。超えたらそれ以上送らず終了（有料化やブロックを避ける）。
 * 個人 Gmail は公式でも日次上限が厳しめのため、余裕を見て低めに設定すること。
 */
var OUTBOX_EMAIL_MAX_PER_DAY = 80;

/** 連投でレート制限になりにくいよう、各送信のあとに空けるミリ秒（0 で無効） */
var OUTBOX_EMAIL_SLEEP_MS = 400;

/**
 * true: 管理者の「発信（broadcast）」実行時に、そのままメール送信まで行う。
 * false: 発信はキュー作成までに留め、手動実行やトリガーで送る。
 */
var BROADCAST_AUTO_SEND_EMAIL = true;

/**
 * true: 管理者の「発信（broadcast）」実行時に、そのままLINE送信まで行う。
 */
var BROADCAST_AUTO_SEND_LINE = false;

/** 配布キューからのLINE一括送信: 1回の実行で処理する最大行数（queued のみ） */
var OUTBOX_LINE_MAX_PER_RUN = 50;

/** 連投でレート制限になりにくいよう、各送信のあとに空けるミリ秒（0 で無効） */
var OUTBOX_LINE_SLEEP_MS = 500;

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
 *
 * LINE Messaging API の Webhook は JSON で events を送る。
 * GAS の doPost では X-Line-Signature ヘッダーが取れないため、
 * Webhook URL にクエリ ?line_webhook_secret=... を付け、スクリプトプロパティ LINE_WEBHOOK_SECRET と一致させて認証する。
 */
function doPost(e) {
  try {
    var lineOut = lineWebhookMaybeHandle_(e);
    if (lineOut) return lineOut;

    var data = parsePostPayload_(e);
    /** 管理者: 発信（トークン発行＋ログ保存＋配布用URL生成） */
    if (String(data.action || '').trim() === 'broadcast') {
      return jsonOutput_(broadcast_(data));
    }
    /** 管理者: 担当する/引き継ぐ */
    if (String(data.action || '').trim() === 'assign') {
      return jsonOutput_(assignCase_(data));
    }
    /** 管理者: 対応チェック更新 */
    if (String(data.action || '').trim() === 'updateChecklist') {
      return jsonOutput_(updateChecklist_(data));
    }
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

/**
 * LINE Webhook の POST なら処理して TextOutput を返す。それ以外は null。
 *
 * 【重要】LINE は Webhook に POST する。GAS の script.google.com/.../exec へ POST すると
 * 先に 302 が返り、LINE の「検証」は失敗しやすい。302 の先（googleusercontent）へ POST すると 405 になる。
 * 対策: Cloudflare Worker 等のプロキシが LINE に 200 を返しつつ GAS へ POST を転送する（line-webhook-proxy/worker.mjs）。
 * runPrintLineWebhookUrlForLineConsole で GAS_TARGET_URL と手順を表示する。
 */
function lineWebhookMaybeHandle_(e) {
  if (!e || !e.postData || !e.postData.contents) return null;
  var raw = String(e.postData.contents || '');
  if (!raw || raw.charAt(0) !== '{') return null;
  var obj;
  try {
    obj = JSON.parse(raw);
  } catch (x) {
    return null;
  }
  if (!obj || !Object.prototype.hasOwnProperty.call(obj, 'events') || Object.prototype.toString.call(obj.events) !== '[object Array]') {
    return null;
  }

  var p = e.parameter || {};
  var provided = String(p.line_webhook_secret || p.lineWebhookSecret || p.lw || '').trim();
  var expected = String(PropertiesService.getScriptProperties().getProperty('LINE_WEBHOOK_SECRET') || '').trim();
  if (!expected) {
    Logger.log('LINE Webhook: LINE_WEBHOOK_SECRET が未設定のため無視しました');
    return ContentService.createTextOutput('OK');
  }
  if (provided !== expected) {
    Logger.log('LINE Webhook: line_webhook_secret が一致しません');
    return ContentService.createTextOutput('OK');
  }

  var dest = String(obj.destination || '');
  for (var i = 0; i < obj.events.length; i++) {
    try {
      handleLineWebhookEvent_(dest, obj.events[i]);
    } catch (err) {
      Logger.log('LINE Webhook event error: ' + String(err.message || err));
    }
  }
  return ContentService.createTextOutput('OK');
}

/**
 * エディタから実行: LINE Webhook 用の **GAS 転送先 URL** と、プロキシ（Worker）の手順を表示する。
 *
 * 以前案内した「googleusercontent の URL を LINE に貼る」方式は POST で 405 になるため使えません。
 * Cloudflare Worker（line-webhook-proxy/worker.mjs）をデプロイし、Worker の URL を LINE の Webhook に貼ってください。
 */
function runPrintLineWebhookUrlForLineConsole() {
  var props = PropertiesService.getScriptProperties();
  var secret = String(props.getProperty('LINE_WEBHOOK_SECRET') || '').trim();
  if (!secret) throw new Error('スクリプトのプロパティ LINE_WEBHOOK_SECRET を設定してください');

  var execBase = String(props.getProperty('GAS_WEBAPP_EXEC_URL') || '').trim();
  if (!execBase) {
    throw new Error(
      'スクリプトのプロパティ GAS_WEBAPP_EXEC_URL を設定してください（例: https://script.google.com/macros/s/.../exec）'
    );
  }
  execBase = execBase.replace(/\?.*$/, '').replace(/\/$/, '');
  if (execBase.indexOf('/exec') === -1) {
    throw new Error('GAS_WEBAPP_EXEC_URL は .../exec で終わるウェブアプリの URL にしてください');
  }

  var gasTargetUrl = execBase + '?line_webhook_secret=' + encodeURIComponent(secret);

  var lines = [];
  lines.push('=== LINE Webhook と GAS（重要）===');
  lines.push('');
  lines.push('1) GAS の /exec に POST すると 302 になり、LINE の検証は失敗しやすいです。');
  lines.push('2) 302 の先（googleusercontent）の URL に POST すると 405 になり、LINE からは使えません。');
  lines.push('3) 対策: リポジトリの line-webhook-proxy/worker.mjs を Cloudflare Worker にデプロイしてください。');
  lines.push('');
  lines.push('--- Cloudflare Worker の環境変数「GAS_TARGET_URL」に設定する値（コピー用）---');
  lines.push(gasTargetUrl);
  lines.push('');
  lines.push('--- LINE Developers の「Webhook URL」に貼る値 ---');
  lines.push('Worker デプロイ後に表示される URL（例: https://line-gas-bridge.◯◯◯.workers.dev）');
  lines.push('（上の GAS URL は貼らない。Worker の URL だけを貼る）');
  lines.push('');
  lines.push('GAS のコードを再デプロイしたら、必要に応じて GAS_TARGET_URL を更新してください。');

  var text = lines.join('\n');
  Logger.log(text);
  /**
   * getUi().alert は「OK を押すまで」処理が止まる。エディタ実行でダイアログが表示されない／背面に隠れる
   * と 6 分でタイムアウトしやすい。全文は Logger、通知は非ブロッキングの toast のみにする。
   */
  try {
    var ss = SpreadsheetApp.getActiveSpreadsheet();
    if (ss) {
      ss.toast('GAS_TARGET_URL など全文は「表示」→「ログ」でコピーできます。', 'LINE Webhook URL', 12);
    }
  } catch (e) {}
}

/**
 * follow / message などを1行ログに残す。テキストが「社員番号のみ」なら社員マスターへ line_user_id を書く。
 */
function handleLineWebhookEvent_(destination, ev) {
  if (!ev) return;
  var type = String(ev.type || '').trim();
  var src = ev.source || {};
  var userId = String(src.userId || '').trim();
  var msgText = '';
  if (type === 'message' && ev.message && String(ev.message.type || '') === 'text') {
    msgText = String(ev.message.text || '').trim();
  }

  var ss = getSpreadsheetForWebhook_();
  var sh = getOrCreateSheet_(ss, SHEET_LINE_WEBHOOK_LOG, [
    'received_at',
    'event_type',
    'line_user_id',
    'message_text',
    'destination',
    'linked_employee_id',
  ]);

  var linked = '';
  if (userId && msgText) {
    var linkedId = tryLinkLineUserToEmployee_(ss, userId, msgText);
    if (linkedId) linked = linkedId;
  }

  sh.appendRow([new Date(), type, userId, msgText, destination, linked]);
}

/**
 * メッセージが「社員番号のみ」（数字 3〜10 桁）のとき、社員マスターの line_user_id を更新する。
 * @returns {string} 紐付けた社員番号。紐付けなしは ''。
 */
function tryLinkLineUserToEmployee_(ss, lineUserId, messageText) {
  var id = String(messageText || '').trim();
  if (!/^[0-9]{3,10}$/.test(id)) return '';

  var sh = getSheet_(ss, SHEET_EMPLOYEES);
  var range = sh.getDataRange();
  var values = range.getValues();
  if (values.length < 2) return '';
  var headers = values[0].map(function (h) {
    return String(h).trim();
  });
  var cEmp = headerIndex_(headers, ['社員番号', 'employee_id', 'e_id']);
  var cLine = headerIndex_(headers, ['line_user_id', 'LINE', 'lineUserId']);
  if (cEmp < 0 || cLine < 0) return '';

  var hitRow = -1;
  for (var r = 1; r < values.length; r++) {
    var eid = String(values[r][cEmp] || '').trim();
    if (eid === id) {
      if (hitRow > 0) return '';
      hitRow = r + 1;
    }
  }
  if (hitRow < 0) return '';

  sh.getRange(hitRow, cLine + 1).setValue(lineUserId);
  return id;
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
  var defaultOwnerByEmp = buildDefaultOwnerMap_(targets);
  var assignments = readAssignmentsForSession_(ss, sessionId);

  targetIds.forEach(function (eid) {
    var r = respByEmp[eid];
    var emp = empById[eid] || {};
    var name = emp['氏名'] || emp['name'] || '';
    var dept = emp['部署'] || emp['department'] || '';
    var office = emp['拠点'] || emp['office'] || '';
    var phone = emp['電話番号'] || emp['phone'] || '';
    var emergency = emp['緊急連絡先'] || emp['emergency_contact'] || '';
    var defaultOwner = defaultOwnerByEmp[eid] || '';

    if (!r) {
      var a0 = assignments[eid + ':no_response'] || null;
      noResponseList.push({
        employeeId: eid,
        name: name,
        department: dept,
        office: office,
        phone: phone,
        emergency_contact: emergency,
        default_owner: defaultOwner,
        current_owner: a0 ? String(a0.current_owner || '') : '',
        checklist: a0
          ? { ambulance: !!a0.ambulance_called, emergency: !!a0.emergency_contact_called }
          : { ambulance: false, emergency: false },
        updatedAt: a0 ? formatDate_(a0.updated_at) : '',
        updatedBy: a0 ? String(a0.updated_by || '') : '',
      });
      return;
    }

    var st = normalizeStatus_(r['ステータス'] || r['status']);
    /** レスポンス行はあるがステータスが空＝未回答扱い（下書き行など） */
    if (!st) {
      var a1 = assignments[eid + ':no_response'] || null;
      noResponseList.push({
        employeeId: eid,
        name: name,
        department: dept,
        office: office,
        phone: phone,
        emergency_contact: emergency,
        default_owner: defaultOwner,
        current_owner: a1 ? String(a1.current_owner || '') : '',
        checklist: a1
          ? { ambulance: !!a1.ambulance_called, emergency: !!a1.emergency_contact_called }
          : { ambulance: false, emergency: false },
        updatedAt: a1 ? formatDate_(a1.updated_at) : '',
        updatedBy: a1 ? String(a1.updated_by || '') : '',
      });
      return;
    }

    if (byStatus[st] === undefined) byStatus.unknown++;
    else byStatus[st]++;

    if (st === 'need_help') {
      var a2 = assignments[eid + ':need_help'] || null;
      needHelpList.push({
        employeeId: eid,
        name: name,
        department: dept,
        office: office,
        phone: phone,
        emergency_contact: emergency,
        default_owner: defaultOwner,
        current_owner: a2 ? String(a2.current_owner || '') : '',
        checklist: a2
          ? { ambulance: !!a2.ambulance_called, emergency: !!a2.emergency_contact_called }
          : { ambulance: false, emergency: false },
        updatedAt: a2 ? formatDate_(a2.updated_at) : '',
        updatedBy: a2 ? String(a2.updated_by || '') : '',
        comment: r['コメント'] || r['comment'] || '',
        answeredAt: formatDate_(r['回答日時'] || r['answered_at']),
        response_channel: r['回答チャネル'] || r['response_channel'] || '',
      });
    }
  });

  var answered = targetIds.length - noResponseList.length;
  var byDepartment = buildDepartmentBreakdown_(targets, respByEmp);
  var byOffice = buildOfficeRates_(targets, respByEmp);

  return {
    sessionId: sessionId,
    session: {
      title: sessionRow['タイトル'] || sessionRow['title'] || '',
      status: sessionRow['状態'] || sessionRow['status'] || '',
      targetCountSheet: sessionRow['対象人数'] || sessionRow['target_count'] || '',
      dueAt: formatDate_(sessionRow['回答期限'] || sessionRow['response_due_at'] || sessionRow['due_at'] || sessionRow['dueAt']),
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
    byOffice: byOffice,
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

function buildOfficeRates_(targets, respByEmp) {
  var map = {};
  targets.forEach(function (emp) {
    var office = String(emp['拠点'] || emp['office'] || '（未設定）').trim() || '（未設定）';
    if (!map[office]) map[office] = { office: office, answered: 0, total: 0 };
    var eid = String(emp['社員番号'] || emp['employee_id'] || '').trim();
    map[office].total++;
    if (respByEmp[eid] && normalizeStatus_(respByEmp[eid]['ステータス'] || respByEmp[eid]['status'])) map[office].answered++;
  });
  return Object.keys(map)
    .sort()
    .map(function (k) {
      var o = map[k];
      o.rate = o.total ? Math.round((1000 * o.answered) / o.total) / 10 : 0;
      return o;
    })
    .sort(function (a, b) {
      /** 未回答が多い（rateが低い）順に並べる */
      return (a.rate || 0) - (b.rate || 0);
    });
}

/**
 * Webhook・ウェブアプリの doPost（発信 broadcast・配布キュー処理など）でブックを取得する。
 * コンテナバインドでは通常 getActiveSpreadsheet で足りる。
 * スタンドアロンでは null になり得るため、その場合はスクリプトプロパティ SPREADSHEET_ID にブック ID を設定する。
 */
function getSpreadsheetForWebhook_() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  if (ss) return ss;
  var id = String(PropertiesService.getScriptProperties().getProperty('SPREADSHEET_ID') || '').trim();
  if (!id) {
    throw new Error(
      'スプレッドシートを開けません。スクリプトがスプレッドシートにコンテナバインドされているか確認するか、スクリプトプロパティ SPREADSHEET_ID にブックの ID を設定してください'
    );
  }
  return SpreadsheetApp.openById(id);
}

function getOrCreateSheet_(ss, name, headers) {
  var sh = ss.getSheetByName(name);
  if (sh) return sh;
  sh = ss.insertSheet(name);
  if (headers && headers.length) sh.appendRow(headers);
  return sh;
}

function isHttpUrl_(s) {
  var t = String(s || '').trim();
  if (!t) return false;
  return /^https?:\/\//i.test(t);
}

function getRequiredScriptProp_(key) {
  var v = PropertiesService.getScriptProperties().getProperty(key);
  v = String(v || '').trim();
  if (!v) throw new Error('スクリプトのプロパティに ' + key + ' が設定されていません');
  return v;
}

function lineGetToken_() {
  return getRequiredScriptProp_('LINE_CHANNEL_ACCESS_TOKEN');
}

function lineApiFetch_(path, method, payloadObj) {
  var url = 'https://api.line.me' + path;
  var params = {
    method: method || 'get',
    muteHttpExceptions: true,
    headers: {
      Authorization: 'Bearer ' + lineGetToken_(),
    },
  };
  if (payloadObj !== undefined) {
    params.contentType = 'application/json';
    params.payload = JSON.stringify(payloadObj);
  }
  var res = UrlFetchApp.fetch(url, params);
  var code = res.getResponseCode();
  var body = res.getContentText();
  var json = null;
  try {
    json = body ? JSON.parse(body) : null;
  } catch (e) {
    json = null;
  }
  if (code < 200 || code >= 300) {
    throw new Error('LINE API ' + code + ': ' + (body || ''));
  }
  return json;
}

function lineGetQuotaRemaining_() {
  /** 無料枠/上限超過を避けるため、上限と消費を取りに行く。 */
  try {
    var quota = lineApiFetch_('/v2/bot/message/quota', 'get');
    var consumption = lineApiFetch_('/v2/bot/message/quota/consumption', 'get');
    var limit = quota && quota.value !== undefined ? parseInt(String(quota.value), 10) : NaN;
    var used = consumption && consumption.totalUsage !== undefined ? parseInt(String(consumption.totalUsage), 10) : NaN;
    if (isNaN(limit) || isNaN(used)) return 0;
    return Math.max(0, limit - used);
  } catch (e) {
    /**
     * トークン不正・プランによって quota API が取れない場合、旧実装は 0 を返して「1通も送らない」になっていた。
     * 実送信は Push の成否で分かるため、取得失敗時は打ち切らず試行する（無料枠超過は Push が 429 等になる）。
     */
    Logger.log('LINE quota API を取得できませんでした（送信は試行します）: ' + String(e.message || e));
    return 999999;
  }
}

function broadcast_(data) {
  var ss = getSpreadsheetForWebhook_();
  var sessionId = String(data.session_id || data.sessionId || '').trim();
  if (!sessionId) throw new Error('sessionId が必要です');

  /** 配布する回答フォームURL（respond.html のURL）。無いならログだけ残す。 */
  var respondBaseUrl = String(data.respond_base_url || data.respondBaseUrl || '').trim();
  if (respondBaseUrl && !isHttpUrl_(respondBaseUrl)) {
    throw new Error(
      '回答フォーム URL（respond.html）は http(s) の公開URLにしてください。' +
        ' file:/// やローカルパスは受信者が開けません（GitHub Pages などを使用してください）'
    );
  }
  var title = String(data.title || '').trim();
  var body = String(data.body || data.message || '').trim();
  var dueAtRaw = data.due_at || data.dueAt || data.回答期限 || data.response_due_at;
  var dueAt = parseDateish_(dueAtRaw);
  var createSession = String(data.create_session || data.createSession || '').trim();
  /** デフォルトは作成する（管理者がスプレッドシートを触らない運用を優先） */
  if (createSession === '') createSession = 'true';
  var willCreate = String(createSession).toLowerCase() !== 'false';

  /** セッションが無ければ作成（あれば更新） */
  if (willCreate) {
    upsertSessionRow_(ss, sessionId, title, dueAt);
  } else {
    if (!findSessionRow_(ss, sessionId)) throw new Error('セッションが見つかりません: ' + sessionId);
  }

  var issued = issueTokensForSession(sessionId);

  var links = [];
  if (respondBaseUrl) {
    var base = respondBaseUrl.replace(/\?.*$/, '');
    issued.forEach(function (x) {
      links.push({
        employee_id: x.employee_id,
        url: base + '?token=' + encodeURIComponent(String(x.token || '').trim()),
      });
    });
  }

  var sh = getOrCreateSheet_(ss, SHEET_BROADCASTS, [
    'created_at',
    'session_id',
    'title',
    'body',
    'respond_base_url',
    'issued_count',
  ]);
  sh.appendRow([new Date(), sessionId, title, body, respondBaseUrl, issued.length]);

  /** 配布キューに全員分の「配布対象」を書き出す（送信処理は別実装） */
  var outboxCount = writeOutbox_(ss, sessionId, title, body, links);

  var mailResult = null;
  if (BROADCAST_AUTO_SEND_EMAIL) {
    /** 無料枠・日次上限を超えそうなら内部で打ち切る。失敗行の自動再送はしない。 */
    mailResult = processOutboxEmail_(sessionId);
  }

  var lineResult = null;
  if (BROADCAST_AUTO_SEND_LINE) {
    lineResult = processOutboxLine_(sessionId);
  }

  /** 大量データを返しすぎない（UI側で必要ならCSV化などに拡張） */
  var preview = links.slice(0, 30);
  return {
    ok: true,
    action: 'broadcast',
    session_id: sessionId,
    issued_count: issued.length,
    outbox_count: outboxCount,
    mail_result: mailResult,
    line_result: lineResult,
    respond_base_url: respondBaseUrl,
    links_preview: preview,
    note:
      'このAPIは「セッション作成/更新 → トークン再発行 → 配布キュー作成 → 発信ログ記録 → 配布用URL生成」までを行います。BROADCAST_AUTO_SEND_EMAIL=true の場合は、続けてメール送信も行います（無料枠/上限で途中停止することがあります）。',
  };
}

function upsertSessionRow_(ss, sessionIdStr, title, dueAtMaybe_) {
  var sh = getSheet_(ss, SHEET_SESSIONS);
  var range = sh.getDataRange();
  var values = range.getValues();
  if (!values.length) throw new Error('セッションシートが空です。1行目にヘッダーを入れてください');
  var headers = values[0].map(function (h) {
    return String(h).trim();
  });

  var cSid = headerIndex_(headers, ['セッションID', 'session_ID', 'session_id', 'sessionId']);
  if (cSid < 0) throw new Error('セッションシートにセッションID列（session_id 等）がありません');
  var cTitle = headerIndex_(headers, ['タイトル', 'title']);
  var cStatus = headerIndex_(headers, ['状態', 'status']);
  var cCreated = headerIndex_(headers, ['作成日時', 'created_at', 'createdAt', '送信開始日時', 'send_started_at']);
  var cTarget = headerIndex_(headers, ['対象人数', 'target_count', 'targetCount']);
  var cDue = headerIndex_(headers, ['回答期限', 'response_due_at', 'due_at', 'dueAt']);

  var now = new Date();
  var rowIndex = -1;
  for (var r = 1; r < values.length; r++) {
    if (String(values[r][cSid] || '').trim() === sessionIdStr) {
      rowIndex = r + 1;
      break;
    }
  }

  if (rowIndex > 0) {
    if (cTitle >= 0 && title) sh.getRange(rowIndex, cTitle + 1).setValue(title);
    if (cStatus >= 0) sh.getRange(rowIndex, cStatus + 1).setValue('受付中');
    if (cCreated >= 0 && !sh.getRange(rowIndex, cCreated + 1).getValue())
      sh.getRange(rowIndex, cCreated + 1).setValue(now);
    if (cTarget >= 0)
      sh.getRange(rowIndex, cTarget + 1).setValue(readObjects_(getSheet_(ss, SHEET_EMPLOYEES)).filter(isActiveEmployee_).length);
    if (cDue >= 0 && dueAtMaybe_ !== undefined) {
      if (dueAtMaybe_) sh.getRange(rowIndex, cDue + 1).setValue(dueAtMaybe_);
      else sh.getRange(rowIndex, cDue + 1).setValue('');
    }
    return;
  }

  var newRow = [];
  for (var i = 0; i < headers.length; i++) newRow.push('');
  newRow[cSid] = sessionIdStr;
  if (cTitle >= 0) newRow[cTitle] = title || '';
  if (cStatus >= 0) newRow[cStatus] = '受付中';
  if (cCreated >= 0) newRow[cCreated] = now;
  if (cTarget >= 0) newRow[cTarget] = readObjects_(getSheet_(ss, SHEET_EMPLOYEES)).filter(isActiveEmployee_).length;
  if (cDue >= 0 && dueAtMaybe_) newRow[cDue] = dueAtMaybe_;
  sh.appendRow(newRow);
}

function parseDateish_(v) {
  if (v === null || v === undefined || v === '') return null;
  if (Object.prototype.toString.call(v) === '[object Date]' && !isNaN(v.getTime())) return v;
  /** issue.html は datetime-local を送る（例: 2026-05-07T19:30） */
  var s = String(v).trim();
  if (!s) return null;
  var d = new Date(s);
  if (!isNaN(d.getTime())) return d;
  /** "yyyy/MM/dd HH:mm" なども一応吸収 */
  var normalized = s.replace(/\//g, '-').replace(' ', 'T');
  d = new Date(normalized);
  if (!isNaN(d.getTime())) return d;
  return null;
}

/**
 * 社員マスター1行（readObjects_ のオブジェクト）からメールを取り出す。
 * ヘッダーが Email / メールアドレス 等でも拾えるようにする。
 */
function employeeEmailFromRow_(emp) {
  if (!emp) return '';
  var candidates = [
    'email',
    'Email',
    'EMAIL',
    'e-mail',
    'E-mail',
    'mail',
    'Mail',
    'メール',
    'メールアドレス',
    'メールアドレス（会社）',
    '会社メール',
  ];
  for (var i = 0; i < candidates.length; i++) {
    var k = candidates[i];
    if (!Object.prototype.hasOwnProperty.call(emp, k)) continue;
    var v = emp[k];
    if (v === undefined || v === null) continue;
    var s = String(v).trim();
    if (s) return s;
  }
  for (var key in emp) {
    if (!Object.prototype.hasOwnProperty.call(emp, key)) continue;
    var kn = String(key).trim();
    var lower = kn.toLowerCase();
    if (lower.indexOf('mail') !== -1 || kn.indexOf('メール') !== -1) {
      var vv = emp[key];
      if (vv === undefined || vv === null) continue;
      var t = String(vv).trim();
      if (t) return t;
    }
  }
  return '';
}

function writeOutbox_(ss, sessionIdStr, title, body, links) {
  var sh = getOrCreateSheet_(ss, SHEET_OUTBOX, [
    'created_at',
    'session_id',
    'employee_id',
    'name',
    'department',
    'office',
    'email',
    'phone',
    'line_user_id',
    'respond_url',
    'title',
    'body',
    'status',
    'mail_error',
    'mail_sent_at',
  ]);

  ensureOutboxMailMetaColumns_(sh);

  /** 今回セッション分の既存キューを削除して入れ替え */
  var values = sh.getDataRange().getValues();
  if (values.length >= 2) {
    var headers = values[0].map(function (h) {
      return String(h).trim();
    });
    var cSid = headerIndex_(headers, ['session_id', 'セッションID', 'session_ID', 'sessionId']);
    if (cSid >= 0) {
      for (var r = values.length; r >= 2; r--) {
        if (String(values[r - 1][cSid] || '').trim() === sessionIdStr) sh.deleteRow(r);
      }
    }
  }

  var employees = readObjects_(getSheet_(ss, SHEET_EMPLOYEES)).filter(isActiveEmployee_);
  var linkByEmp = {};
  (links || []).forEach(function (x) {
    if (x && x.employee_id) linkByEmp[String(x.employee_id).trim()] = String(x.url || '').trim();
  });

  var now = new Date();
  var count = 0;
  employees.forEach(function (emp) {
    var eid = String(emp['社員番号'] || emp['employee_id'] || '').trim();
    if (!eid) return;
    var url = linkByEmp[eid] || '';
    sh.appendRow([
      now,
      sessionIdStr,
      eid,
      emp['氏名'] || emp['name'] || '',
      emp['部署'] || emp['department'] || '',
      emp['拠点'] || emp['office'] || '',
      employeeEmailFromRow_(emp),
      emp['電話番号'] || emp['phone'] || '',
      emp['line_user_id'] || emp['LINE'] || '',
      url,
      title || '',
      body || '',
      'queued',
      '',
      '',
    ]);
    count++;
  });
  return count;
}

/**
 * 既存の「配布キュー」に mail_error / mail_sent_at 列が無い場合だけ 1 行目に追加する。
 */
function ensureOutboxMailMetaColumns_(sh) {
  var lastCol = Math.max(sh.getLastColumn(), 1);
  var headers = sh.getRange(1, 1, 1, lastCol)
    .getValues()[0]
    .map(function (h) {
      return String(h).trim();
    });
  var cErr = headerIndex_(headers, ['mail_error', 'メール送信エラー']);
  var cAt = headerIndex_(headers, ['mail_sent_at', 'メール送信日時']);
  var appendAt = lastCol;
  if (cErr < 0) {
    appendAt++;
    sh.getRange(1, appendAt).setValue('mail_error');
    headers.push('mail_error');
    cErr = headers.length - 1;
  }
  if (cAt < 0) {
    appendAt++;
    sh.getRange(1, appendAt).setValue('mail_sent_at');
  }
}

var PROP_OUTBOX_EMAIL_DAY_ = 'OUTBOX_EMAIL_SENT_DAY';
var PROP_OUTBOX_EMAIL_COUNT_ = 'OUTBOX_EMAIL_SENT_COUNT';

function todayKeyForQuota_() {
  return Utilities.formatDate(new Date(), Session.getScriptTimeZone(), 'yyyy-MM-dd');
}

function getOutboxEmailSentCountToday_() {
  var props = PropertiesService.getScriptProperties();
  var day = todayKeyForQuota_();
  if (props.getProperty(PROP_OUTBOX_EMAIL_DAY_) !== day) return 0;
  var n = parseInt(String(props.getProperty(PROP_OUTBOX_EMAIL_COUNT_) || '0'), 10);
  return isNaN(n) ? 0 : n;
}

function addOutboxEmailSentCountToday_(delta) {
  var props = PropertiesService.getScriptProperties();
  var day = todayKeyForQuota_();
  if (props.getProperty(PROP_OUTBOX_EMAIL_DAY_) !== day) {
    props.setProperty(PROP_OUTBOX_EMAIL_DAY_, day);
    props.setProperty(PROP_OUTBOX_EMAIL_COUNT_, String(Math.max(0, delta)));
    return;
  }
  var cur = getOutboxEmailSentCountToday_();
  props.setProperty(PROP_OUTBOX_EMAIL_COUNT_, String(cur + Math.max(0, delta)));
}

/**
 * 指定セッションの配布キュー（status が queued）に対し、MailApp でメール送信する。
 * OUTBOX_SEND_ONLY_WITHIN_FREE_TIER が true のとき、日次・1回あたりの上限を超えたら送らず打ち切る。
 * 失敗行は email_failed とし自動再送しない（OUTBOX_NO_AUTO_RETRY）。
 *
 * @param {string} sessionIdStr セッション ID（例: 202605）
 * @returns {{ ok: boolean, sent: number, failed: number, skipped: number, stopped_reason?: string, errors?: string[] }}
 */
function processOutboxEmail_(sessionIdStr) {
  if (!OUTBOX_SEND_ONLY_WITHIN_FREE_TIER) {
    /** プロジェクト方針では常に true。false の場合は上限なしで送る（非推奨） */
  }
  sessionIdStr = String(sessionIdStr || '').trim();
  if (!sessionIdStr) throw new Error('sessionId が空です');

  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var sh = ss.getSheetByName(SHEET_OUTBOX);
  if (!sh) throw new Error('シートがありません: ' + SHEET_OUTBOX);

  ensureOutboxMailMetaColumns_(sh);

  var range = sh.getDataRange();
  var values = range.getValues();
  if (values.length < 2) return { ok: true, sent: 0, failed: 0, skipped: 0, stopped_reason: 'no_rows' };

  var headers = values[0].map(function (h) {
    return String(h).trim();
  });
  var cSid = headerIndex_(headers, ['session_id', 'セッションID', 'session_ID', 'sessionId']);
  var cEmail = headerIndex_(headers, ['email', 'メール', 'Email', 'MAIL', 'メールアドレス']);
  var cTitle = headerIndex_(headers, ['title', '件名']);
  var cBody = headerIndex_(headers, ['body', '本文']);
  var cUrl = headerIndex_(headers, ['respond_url', '回答URL']);
  var cStatus = headerIndex_(headers, ['status', '状態']);
  var cErr = headerIndex_(headers, ['mail_error', 'メール送信エラー']);
  var cAt = headerIndex_(headers, ['mail_sent_at', 'メール送信日時']);
  if (cSid < 0 || cEmail < 0 || cUrl < 0 || cStatus < 0) throw new Error('配布キューに必要な列がありません（session_id / email / respond_url / status）');

  var sent = 0;
  var failed = 0;
  var skipped = 0;
  var errors = [];
  var processed = 0;
  var stoppedReason = '';

  for (var r = 1; r < values.length; r++) {
    if (processed >= OUTBOX_EMAIL_MAX_PER_RUN) {
      stoppedReason = 'max_per_run';
      break;
    }
    var row = values[r];
    if (String(row[cSid] || '').trim() !== sessionIdStr) continue;
    var st = String(row[cStatus] || '').trim().toLowerCase();
    /**
     * 先にメール送信が走ると status が email_sent / email_skipped_* になり queued ではなくなる。
     * その場合も LINE は送れるので、email_* も対象に含める。
     * 既に line_* になっている行は二重送信を避けるため対象外にする。
     */
    if (st.indexOf('line_') === 0) continue;
    if (!(st === 'queued' || st.indexOf('email_') === 0)) continue;

    processed++;

    if (OUTBOX_SEND_ONLY_WITHIN_FREE_TIER && getOutboxEmailSentCountToday_() >= OUTBOX_EMAIL_MAX_PER_DAY) {
      stoppedReason = 'max_per_day';
      errors.push('本日の送信上限（' + OUTBOX_EMAIL_MAX_PER_DAY + '通）に達したため中止しました');
      break;
    }

    var to = String(row[cEmail] || '').trim();
    var url = String(row[cUrl] || '').trim();
    var subj = cTitle >= 0 ? String(row[cTitle] || '').trim() : '';
    var bodyText = cBody >= 0 ? String(row[cBody] || '').trim() : '';
    if (!subj) subj = '【安否確認】回答のお願い';

    if (!to || !isPlausibleEmail_(to)) {
      skipped++;
      sh.getRange(r + 1, cStatus + 1).setValue('email_skipped_no_address');
      if (cErr >= 0) sh.getRange(r + 1, cErr + 1).setValue('メールアドレスが空、または形式が不正です');
      continue;
    }

    if (!url) {
      skipped++;
      sh.getRange(r + 1, cStatus + 1).setValue('email_skipped_no_address');
      if (cErr >= 0) sh.getRange(r + 1, cErr + 1).setValue('回答URL（respond_url）が空です');
      continue;
    }

    var mailBody =
      (bodyText ? bodyText + '\n\n' : '') +
      '以下のURLから回答してください。\n' +
      url +
      '\n\n----\nセッション: ' +
      sessionIdStr;

    try {
      MailApp.sendEmail({ to: to, subject: subj, body: mailBody });
      sh.getRange(r + 1, cStatus + 1).setValue('email_sent');
      if (cErr >= 0) sh.getRange(r + 1, cErr + 1).setValue('');
      if (cAt >= 0) sh.getRange(r + 1, cAt + 1).setValue(new Date());
      sent++;
      if (OUTBOX_SEND_ONLY_WITHIN_FREE_TIER) addOutboxEmailSentCountToday_(1);
      if (OUTBOX_EMAIL_SLEEP_MS > 0) Utilities.sleep(OUTBOX_EMAIL_SLEEP_MS);
    } catch (err) {
      failed++;
      var msg = String(err.message || err);
      sh.getRange(r + 1, cStatus + 1).setValue('email_failed');
      if (cErr >= 0) sh.getRange(r + 1, cErr + 1).setValue(msg);
      errors.push('行 ' + (r + 1) + ': ' + msg);
      if (OUTBOX_NO_AUTO_RETRY) {
        /** 1件失敗しても他行は続ける。日次上限は別チェック。 */
      }
    }
  }

  return {
    ok: failed === 0,
    sent: sent,
    failed: failed,
    skipped: skipped,
    stopped_reason: stoppedReason || undefined,
    errors: errors.length ? errors : undefined,
  };
}

/** 厳密な RFC 検証ではなく、明らかな誤入力を弾く */
function isPlausibleEmail_(s) {
  if (!s || s.indexOf('@') < 1 || s.indexOf('@') === s.length - 1) return false;
  if (/\s/.test(s)) return false;
  return true;
}

/**
 * 既存の「配布キュー」に line_error / line_sent_at 列が無い場合だけ 1 行目に追加する。
 */
function ensureOutboxLineMetaColumns_(sh) {
  var lastCol = Math.max(sh.getLastColumn(), 1);
  var headers = sh.getRange(1, 1, 1, lastCol)
    .getValues()[0]
    .map(function (h) {
      return String(h).trim();
    });
  var cErr = headerIndex_(headers, ['line_error', 'LINE送信エラー']);
  var cAt = headerIndex_(headers, ['line_sent_at', 'LINE送信日時']);
  var appendAt = lastCol;
  if (cErr < 0) {
    appendAt++;
    sh.getRange(1, appendAt).setValue('line_error');
    headers.push('line_error');
    cErr = headers.length - 1;
  }
  if (cAt < 0) {
    appendAt++;
    sh.getRange(1, appendAt).setValue('line_sent_at');
  }
}

function buildLineMessageText_(subj, bodyText, url) {
  var parts = [];
  if (subj) parts.push(String(subj).trim());
  if (bodyText) parts.push(String(bodyText).trim());
  if (url) parts.push(String(url).trim());
  var text = parts.filter(Boolean).join('\n\n');
  /** LINE の text 上限に収める（厳密ではないが安全側） */
  var maxLen = 4500;
  if (text.length > maxLen) text = text.slice(0, maxLen - 1) + '…';
  return text;
}

/**
 * 指定セッションの配布キュー（status が queued）に対し、LINE へ push 送信する。
 * OUTBOX_SEND_ONLY_WITHIN_FREE_TIER が true のとき、quota の残量が足りない場合は送らず打ち切る（安全側）。
 *
 * 前提: スクリプトのプロパティに LINE_CHANNEL_ACCESS_TOKEN を設定していること。
 *
 * @param {string} sessionIdStr セッション ID
 * @returns {{ ok: boolean, sent: number, failed: number, skipped: number, stopped_reason?: string, errors?: string[], quota_remaining?: number }}
 */
function processOutboxLine_(sessionIdStr) {
  sessionIdStr = String(sessionIdStr || '').trim();
  if (!sessionIdStr) throw new Error('sessionId が空です');

  var ss = getSpreadsheetForWebhook_();
  var sh = ss.getSheetByName(SHEET_OUTBOX);
  if (!sh) throw new Error('シートがありません: ' + SHEET_OUTBOX);

  ensureOutboxLineMetaColumns_(sh);

  var range = sh.getDataRange();
  var values = range.getValues();
  if (values.length < 2) return { ok: true, sent: 0, failed: 0, skipped: 0, stopped_reason: 'no_rows', quota_remaining: 0 };

  var headers = values[0].map(function (h) {
    return String(h).trim();
  });
  var cSid = headerIndex_(headers, ['session_id', 'セッションID', 'session_ID', 'sessionId']);
  var cLine = headerIndex_(headers, ['line_user_id', 'LINE', 'lineUserId']);
  var cTitle = headerIndex_(headers, ['title', '件名']);
  var cBody = headerIndex_(headers, ['body', '本文']);
  var cUrl = headerIndex_(headers, ['respond_url', '回答URL']);
  var cStatus = headerIndex_(headers, ['status', '状態']);
  var cErr = headerIndex_(headers, ['line_error', 'LINE送信エラー']);
  var cAt = headerIndex_(headers, ['line_sent_at', 'LINE送信日時']);
  if (cSid < 0 || cLine < 0 || cUrl < 0 || cStatus < 0) throw new Error('配布キューに必要な列がありません（session_id / line_user_id / respond_url / status）');

  var remaining = OUTBOX_SEND_ONLY_WITHIN_FREE_TIER ? lineGetQuotaRemaining_() : 9999999;
  var sent = 0;
  var failed = 0;
  var skipped = 0;
  var errors = [];
  var processed = 0;
  var stoppedReason = '';

  for (var r = 1; r < values.length; r++) {
    if (processed >= OUTBOX_LINE_MAX_PER_RUN) {
      stoppedReason = 'max_per_run';
      break;
    }
    var row = values[r];
    if (String(row[cSid] || '').trim() !== sessionIdStr) continue;
    var st = String(row[cStatus] || '').trim().toLowerCase();
    /**
     * broadcast_ は writeOutbox_ のあと processOutboxEmail_ を先に実行することがある。
     * メール送信後は status が email_* になり、この時点では queued ではないため、旧実装だと LINE が1通も送られない。
     * メール側と同様に queued と email_* を対象にし、既に line_* の行は二重送信しない。
     */
    if (st.indexOf('line_') === 0) continue;
    if (!(st === 'queued' || st.indexOf('email_') === 0)) continue;

    processed++;

    if (OUTBOX_SEND_ONLY_WITHIN_FREE_TIER && remaining <= 0) {
      stoppedReason = 'line_quota_exhausted';
      errors.push('LINE の送信枠（quota）が不足しているため中止しました');
      break;
    }

    var to = String(row[cLine] || '').trim();
    var url = String(row[cUrl] || '').trim();
    var subj = cTitle >= 0 ? String(row[cTitle] || '').trim() : '';
    var bodyText = cBody >= 0 ? String(row[cBody] || '').trim() : '';
    if (!subj) subj = '【安否確認】回答のお願い';

    if (!to) {
      skipped++;
      sh.getRange(r + 1, cStatus + 1).setValue('line_skipped_no_userid');
      if (cErr >= 0) sh.getRange(r + 1, cErr + 1).setValue('LINE の宛先（line_user_id）が空です');
      continue;
    }
    if (!url || !isHttpUrl_(url)) {
      skipped++;
      sh.getRange(r + 1, cStatus + 1).setValue('line_skipped_no_url');
      if (cErr >= 0) sh.getRange(r + 1, cErr + 1).setValue('回答URL（respond_url）が空、または http(s) ではありません');
      continue;
    }

    var text = buildLineMessageText_(subj, bodyText, url);

    try {
      lineApiFetch_('/v2/bot/message/push', 'post', {
        to: to,
        messages: [{ type: 'text', text: text }],
      });
      sh.getRange(r + 1, cStatus + 1).setValue('line_sent');
      if (cErr >= 0) sh.getRange(r + 1, cErr + 1).setValue('');
      if (cAt >= 0) sh.getRange(r + 1, cAt + 1).setValue(new Date());
      sent++;
      remaining--;
      if (OUTBOX_LINE_SLEEP_MS > 0) Utilities.sleep(OUTBOX_LINE_SLEEP_MS);
    } catch (err) {
      failed++;
      var msg = String(err.message || err);
      sh.getRange(r + 1, cStatus + 1).setValue('line_failed');
      if (cErr >= 0) sh.getRange(r + 1, cErr + 1).setValue(msg);
      errors.push('行 ' + (r + 1) + ': ' + msg);
      /** quota 系のエラーっぽい場合は、安全側で停止 */
      if (OUTBOX_SEND_ONLY_WITHIN_FREE_TIER && (/LINE API 429/.test(msg) || /quota/i.test(msg))) {
        stoppedReason = 'line_quota_error';
        break;
      }
    }
  }

  return {
    ok: failed === 0,
    sent: sent,
    failed: failed,
    skipped: skipped,
    stopped_reason: stoppedReason || undefined,
    errors: errors.length ? errors : undefined,
    quota_remaining: OUTBOX_SEND_ONLY_WITHIN_FREE_TIER ? remaining : undefined,
  };
}

/**
 * エディタから実行: 下の SESSION_ID_FOR_OUTBOX_EMAIL を実際の ID に書き換えてから ▶ 実行。
 * 初回は「権限を確認」でメール送信の承認が必要です。
 */
function runProcessOutboxEmailFromEditor() {
  var SESSION_ID_FOR_OUTBOX_EMAIL = '202605';
  var out = processOutboxEmail_(SESSION_ID_FOR_OUTBOX_EMAIL);
  var summary =
    'メール送信結果\n成功: ' +
    out.sent +
    ' / 失敗: ' +
    out.failed +
    ' / スキップ: ' +
    out.skipped +
    (out.stopped_reason ? '\n中止理由: ' + out.stopped_reason : '') +
    (out.errors ? '\n' + out.errors.join('\n') : '');
  Logger.log(JSON.stringify(out, null, 2));
  Logger.log(summary);
  try {
    SpreadsheetApp.getUi().alert(summary);
  } catch (uiErr) {
    /** スプレッドシートを開かずエディタだけ実行した場合などはログのみ */
  }
}

/**
 * エディタから実行: 下の SESSION_ID を実際のセッション ID に書き換えてから ▶ 実行。
 * BROADCAST_AUTO_SEND_LINE を変えずに、配布キュー queued / email_* 行への LINE 送信だけやり直したいとき用。
 */
function runProcessOutboxLineFromEditor() {
  var SESSION_ID_FOR_OUTBOX_LINE = '202605';
  var out = processOutboxLine_(SESSION_ID_FOR_OUTBOX_LINE);
  Logger.log(JSON.stringify(out, null, 2));
  var summary =
    'LINE送信結果\n成功: ' +
    out.sent +
    ' / 失敗: ' +
    out.failed +
    ' / スキップ: ' +
    out.skipped +
    (out.stopped_reason ? '\n中止理由: ' + out.stopped_reason : '') +
    (out.errors ? '\n' + out.errors.join('\n') : '');
  Logger.log(summary);
  try {
    var ss = SpreadsheetApp.getActiveSpreadsheet();
    if (ss) ss.toast(String(summary).slice(0, 250), 'LINE 配布キュー', 15);
  } catch (e) {}
}

function buildDefaultOwnerMap_(targets) {
  var map = {};
  (targets || []).forEach(function (emp) {
    var eid = String(emp['社員番号'] || emp['employee_id'] || '').trim();
    if (!eid) return;
    var owner = String(emp['担当者'] || emp['owner'] || emp['担当'] || '').trim();
    map[eid] = owner;
  });
  return map;
}

function normalizeCaseType_(v) {
  var s = String(v || '').trim();
  if (!s) return '';
  if (s === 'need_help' || s === 'no_response') return s;
  /** 互換: no_response_overdue など */
  if (s.indexOf('no_response') === 0) return 'no_response';
  return s;
}

function readAssignmentsForSession_(ss, sessionIdStr) {
  var sh = ss.getSheetByName(SHEET_ASSIGNMENTS);
  if (!sh) return {};
  var values = sh.getDataRange().getValues();
  if (values.length < 2) return {};
  var headers = values[0].map(function (h) {
    return String(h).trim();
  });
  var cSid = headerIndex_(headers, ['session_id', 'セッションID', 'sessionId', 'session_ID']);
  var cEmp = headerIndex_(headers, ['employee_id', '社員番号']);
  var cType = headerIndex_(headers, ['case_type', '種別', 'type']);
  if (cSid < 0 || cEmp < 0 || cType < 0) return {};

  var idx = function (names) {
    return headerIndex_(headers, names);
  };
  var cDef = idx(['default_owner', '既定担当', 'defaultOwner']);
  var cCur = idx(['current_owner', '担当者', 'currentOwner']);
  var cAmb = idx(['ambulance_called', '救急へ連絡']);
  var cEmg = idx(['emergency_contact_called', '緊急連絡先への連絡']);
  var cSt = idx(['status', '状態']);
  var cUpdAt = idx(['updated_at', '更新日時']);
  var cUpdBy = idx(['updated_by', '更新者']);
  var cNote = idx(['handover_note', '引継メモ']);

  var out = {};
  for (var r = 1; r < values.length; r++) {
    var row = values[r];
    if (String(row[cSid] || '').trim() !== sessionIdStr) continue;
    var eid = String(row[cEmp] || '').trim();
    if (!eid) continue;
    var type = normalizeCaseType_(row[cType]);
    if (!type) continue;
    out[eid + ':' + type] = {
      sheetRow: r + 1,
      session_id: sessionIdStr,
      employee_id: eid,
      case_type: type,
      default_owner: cDef >= 0 ? row[cDef] : '',
      current_owner: cCur >= 0 ? row[cCur] : '',
      ambulance_called: cAmb >= 0 ? isTruthyUsed_(row[cAmb]) : false,
      emergency_contact_called: cEmg >= 0 ? isTruthyUsed_(row[cEmg]) : false,
      status: cSt >= 0 ? row[cSt] : '',
      updated_at: cUpdAt >= 0 ? row[cUpdAt] : '',
      updated_by: cUpdBy >= 0 ? row[cUpdBy] : '',
      handover_note: cNote >= 0 ? row[cNote] : '',
    };
  }
  return out;
}

function ensureAssignmentsSheet_(ss) {
  return getOrCreateSheet_(ss, SHEET_ASSIGNMENTS, [
    'session_id',
    'employee_id',
    'case_type',
    'default_owner',
    'current_owner',
    'ambulance_called',
    'emergency_contact_called',
    'status',
    'updated_at',
    'updated_by',
    'handover_note',
  ]);
}

function upsertAssignment_(ss, sessionIdStr, employeeIdStr, caseType, fields) {
  var sh = ensureAssignmentsSheet_(ss);
  var values = sh.getDataRange().getValues();
  var headers = values[0].map(function (h) {
    return String(h).trim();
  });
  var cSid = headerIndex_(headers, ['session_id']);
  var cEmp = headerIndex_(headers, ['employee_id']);
  var cType = headerIndex_(headers, ['case_type']);

  var rowIndex = -1;
  for (var r = 1; r < values.length; r++) {
    if (
      String(values[r][cSid] || '').trim() === sessionIdStr &&
      String(values[r][cEmp] || '').trim() === employeeIdStr &&
      String(values[r][cType] || '').trim() === caseType
    ) {
      rowIndex = r + 1;
      break;
    }
  }

  if (rowIndex < 0) {
    sh.appendRow([
      sessionIdStr,
      employeeIdStr,
      caseType,
      fields.default_owner || '',
      fields.current_owner || '',
      fields.ambulance_called ? true : false,
      fields.emergency_contact_called ? true : false,
      fields.status || '',
      fields.updated_at || new Date(),
      fields.updated_by || '',
      fields.handover_note || '',
    ]);
    return;
  }

  var setCell = function (colName, val) {
    var c = headerIndex_(headers, [colName]);
    if (c >= 0) sh.getRange(rowIndex, c + 1).setValue(val);
  };
  if (fields.default_owner !== undefined) setCell('default_owner', fields.default_owner);
  if (fields.current_owner !== undefined) setCell('current_owner', fields.current_owner);
  if (fields.ambulance_called !== undefined) setCell('ambulance_called', fields.ambulance_called ? true : false);
  if (fields.emergency_contact_called !== undefined)
    setCell('emergency_contact_called', fields.emergency_contact_called ? true : false);
  if (fields.status !== undefined) setCell('status', fields.status);
  setCell('updated_at', fields.updated_at || new Date());
  setCell('updated_by', fields.updated_by || '');
  if (fields.handover_note !== undefined) setCell('handover_note', fields.handover_note || '');
}

function assignCase_(data) {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var sessionId = String(data.session_id || data.sessionId || '').trim();
  var employeeId = String(data.employee_id || data.employeeId || '').trim();
  var caseType = normalizeCaseType_(data.case_type || data.caseType || '');
  var operator = String(data.operator || data.updated_by || '').trim();
  var note = String(data.handover_note || data.note || '').trim();
  if (!sessionId || !employeeId || !caseType) throw new Error('sessionId / employeeId / caseType が必要です');
  if (!operator) throw new Error('operator（管理者名）が必要です');
  if (!findSessionRow_(ss, sessionId)) throw new Error('セッションが見つかりません: ' + sessionId);
  ensureEmployeeExists_(ss, employeeId);

  var employees = readObjects_(getSheet_(ss, SHEET_EMPLOYEES));
  var defaultOwner = '';
  for (var i = 0; i < employees.length; i++) {
    var eid = String(employees[i]['社員番号'] || employees[i]['employee_id'] || '').trim();
    if (eid === employeeId) {
      defaultOwner = String(employees[i]['担当者'] || employees[i]['owner'] || employees[i]['担当'] || '').trim();
      break;
    }
  }

  upsertAssignment_(ss, sessionId, employeeId, caseType, {
    default_owner: defaultOwner,
    current_owner: operator,
    status: 'assigned',
    updated_at: new Date(),
    updated_by: operator,
    handover_note: note,
  });

  return { ok: true, action: 'assign', session_id: sessionId, employee_id: employeeId, case_type: caseType, current_owner: operator };
}

function updateChecklist_(data) {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var sessionId = String(data.session_id || data.sessionId || '').trim();
  var employeeId = String(data.employee_id || data.employeeId || '').trim();
  var caseType = normalizeCaseType_(data.case_type || data.caseType || '');
  var operator = String(data.operator || data.updated_by || '').trim();
  if (!sessionId || !employeeId || !caseType) throw new Error('sessionId / employeeId / caseType が必要です');
  if (!operator) throw new Error('operator（管理者名）が必要です');

  var amb = data.ambulance_called;
  var emg = data.emergency_contact_called;
  var ambBool = amb === undefined ? undefined : isTruthyUsed_(amb) || amb === true;
  var emgBool = emg === undefined ? undefined : isTruthyUsed_(emg) || emg === true;

  /** 未作成なら作る（担当未割当でもチェックは残せる） */
  upsertAssignment_(ss, sessionId, employeeId, caseType, {
    ambulance_called: ambBool,
    emergency_contact_called: emgBool,
    status: ambBool && emgBool ? 'done' : 'assigned',
    updated_at: new Date(),
    updated_by: operator,
  });

  return { ok: true, action: 'updateChecklist', session_id: sessionId, employee_id: employeeId, case_type: caseType };
}
