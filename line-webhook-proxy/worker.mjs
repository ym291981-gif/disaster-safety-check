/**
 * LINE Messaging API → Google Apps Script（ウェブアプリ doPost）の中継用 Cloudflare Worker
 *
 * 背景:
 * - LINE は Webhook に HTTP POST で JSON を送る（検証も POST）。
 * - GAS の https://script.google.com/.../exec へ POST すると 302 が返り、LINE の検証は失敗しやすい。
 * - 302 の Location（script.googleusercontent.com）へ POST すると 405 になる（POST 不可）。
 * - この Worker は LINE に対しては常に 200 を返し、GAS へは POST を転送する（redirect: manual で 302 応答でも中身は捨てる）。
 *
 * デプロイ手順（概要）:
 * 1) Cloudflare にログイン → Workers & Pages → Create → Hello World をベースに作成
 * 2) このファイルの内容をエディタに貼り替えて Save & Deploy
 * 3) Settings → Variables → Environment variables に GAS_TARGET_URL を追加
 *    値は GAS の .../exec?line_webhook_secret=（スクリプトプロパティと同じ）のフル URL
 * 4) 表示された Worker の URL（https://....workers.dev）を LINE Developers の Webhook URL に貼る
 */

export default {
  async fetch(request, env) {
    if (request.method !== 'POST') {
      return new Response('LINE Webhook は POST のみ対応です', { status: 405 });
    }

    var gasUrl = String(env.GAS_TARGET_URL || '').trim();
    if (!gasUrl) {
      return new Response('Worker の環境変数 GAS_TARGET_URL が未設定です', { status: 500 });
    }

    var body = await request.arrayBuffer();
    var headers = new Headers();
    var ct = request.headers.get('Content-Type');
    if (ct) headers.set('Content-Type', ct);
    var sig = request.headers.get('X-Line-Signature');
    if (sig) headers.set('X-Line-Signature', sig);

    try {
      await fetch(gasUrl, {
        method: 'POST',
        headers: headers,
        body: body,
        redirect: 'manual',
      });
    } catch (err) {
      /** GAS 側エラーでも LINE には 200 を返すと再送が減る。運用ではログで監視すること。 */
    }

    return new Response('OK', { status: 200, headers: { 'Content-Type': 'text/plain; charset=UTF-8' } });
  },
};
