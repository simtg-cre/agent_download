// Slack /패키지 + /매뉴얼 → 분기 처리
// /패키지: GitHub Actions repository_dispatch 트리거 (패키지 정보 갱신)
// /매뉴얼 [키워드 ...]: docs.whatap.io/reference 페이지에서 카테고리 LIKE 검색 (공백 구분 다중 키워드 OR 매칭)

const GITHUB_REPO = 'simtg-cre/agent_download';
const DISPATCH_EVENT_TYPE = 'slack-refresh';
const REFRESH_COMMAND = '/패키지';
const MANUAL_COMMAND = '/매뉴얼';
const DOCS_URL = 'https://docs.whatap.io/reference';
const MAX_SECTION_TEXT = 2900;

export default {
  async fetch(request, env, ctx) {
    if (request.method !== 'POST') {
      return new Response('Method Not Allowed', { status: 405 });
    }

    const body = await request.text();
    const timestamp = request.headers.get('X-Slack-Request-Timestamp');
    const signature = request.headers.get('X-Slack-Signature');

    if (!timestamp || !signature) {
      return new Response('Missing Slack headers', { status: 401 });
    }

    const now = Math.floor(Date.now() / 1000);
    if (Math.abs(now - parseInt(timestamp, 10)) > 300) {
      return new Response('Stale request', { status: 401 });
    }

    const expected = await computeSlackSignature(
      env.SLACK_SIGNING_SECRET,
      timestamp,
      body
    );
    if (!timingSafeEqual(expected, signature)) {
      return new Response('Invalid signature', { status: 401 });
    }

    const params = new URLSearchParams(body);
    const command = params.get('command');
    const userName = params.get('user_name') || 'unknown';
    const text = (params.get('text') || '').trim();
    const responseUrl = params.get('response_url');

    if (command === REFRESH_COMMAND) {
      ctx.waitUntil(triggerGitHubWorkflow(env, userName));
      return jsonResponse({
        response_type: 'in_channel',
        text: `:arrows_counterclockwise: \`${userName}\` 님이 패키지 정보 새로고침을 요청했습니다. 잠시 후 결과가 게시됩니다.`,
      });
    }

    if (command === MANUAL_COMMAND) {
      ctx.waitUntil(handleManualCommand(text, userName, responseUrl));
      return jsonResponse({
        response_type: 'in_channel',
        text: text
          ? `:books: \`${userName}\` 님이 매뉴얼 \`${text}\` 검색을 요청했습니다.`
          : `:books: \`${userName}\` 님이 전체 매뉴얼 목록을 요청했습니다.`,
      });
    }

    return jsonResponse({
      response_type: 'ephemeral',
      text: `알 수 없는 명령: ${command}`,
    });
  },
};

// ─────────────────────────── /패키지 ───────────────────────────

async function triggerGitHubWorkflow(env, userName) {
  const resp = await fetch(
    `https://api.github.com/repos/${GITHUB_REPO}/dispatches`,
    {
      method: 'POST',
      headers: {
        Authorization: `Bearer ${env.GITHUB_TOKEN}`,
        Accept: 'application/vnd.github+json',
        'X-GitHub-Api-Version': '2022-11-28',
        'User-Agent': 'slack-refresh-bridge',
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        event_type: DISPATCH_EVENT_TYPE,
        client_payload: { triggered_by: userName },
      }),
    }
  );
  if (!resp.ok) {
    console.error('GitHub dispatch failed', resp.status, await resp.text());
  }
}

// ─────────────────────────── /매뉴얼 ───────────────────────────

async function handleManualCommand(keyword, userName, responseUrl) {
  try {
    const html = await fetchDocsHtml();
    const rows = parseManualTable(html);
    if (rows.length === 0) {
      await postToResponseUrl(responseUrl, {
        response_type: 'in_channel',
        text: ':warning: 매뉴얼 페이지에서 항목을 찾지 못했습니다 (페이지 구조 변경 가능성).',
      });
      return;
    }

    const filtered = filterByCategory(rows, keyword);

    if (filtered.length === 0) {
      const allCategories = rows.map((r) => r.category).join(', ');
      await postToResponseUrl(responseUrl, {
        response_type: 'in_channel',
        text: `:mag: \`${keyword}\` 와 일치하는 카테고리가 없습니다.\n*사용 가능한 카테고리:* ${allCategories}`,
      });
      return;
    }

    const totalCount = filtered.reduce((s, r) => s + r.items.length, 0);
    const headerText = keyword
      ? `:books: *매뉴얼 검색 결과* — \`${keyword}\` (${filtered.length}개 카테고리, 총 ${totalCount}개)`
      : `:books: *전체 매뉴얼 목록* (${filtered.length}개 카테고리, 총 ${totalCount}개)`;

    const blocks = [
      { type: 'section', text: { type: 'mrkdwn', text: headerText } },
      { type: 'divider' },
    ];

    for (const row of filtered) {
      blocks.push(...buildCategoryBlocks(row));
    }

    blocks.push({ type: 'divider' });
    blocks.push({
      type: 'section',
      text: { type: 'mrkdwn', text: '*복사용 URL 목록*' },
    });
    blocks.push(...buildCopyableSections(filtered));

    await postToResponseUrl(responseUrl, {
      response_type: 'in_channel',
      text: headerText,
      blocks,
    });
  } catch (err) {
    console.error('Manual command failed', err);
    await postToResponseUrl(responseUrl, {
      response_type: 'ephemeral',
      text: `:warning: 매뉴얼 페이지를 가져오지 못했습니다: ${err.message}`,
    });
  }
}

async function fetchDocsHtml() {
  const resp = await fetch(DOCS_URL, {
    headers: { 'User-Agent': 'slack-manual-bridge' },
    cf: { cacheTtl: 0 },
  });
  if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
  return resp.text();
}

function parseManualTable(html) {
  const rows = [];
  const rowRegex =
    /<tr[^>]*>\s*<td[^>]*>([\s\S]*?)<\/td>\s*<td[^>]*>([\s\S]*?)<\/td>\s*<\/tr>/g;
  let m;
  while ((m = rowRegex.exec(html)) !== null) {
    const category = stripTags(m[1]).trim();
    if (!category) continue;
    const items = parseCellItems(m[2]);
    if (items.length > 0) rows.push({ category, items });
  }
  return rows;
}

function parseCellItems(cellHtml) {
  const items = [];
  const chunks = cellHtml.split(/\s*\|\s*/);
  for (const chunk of chunks) {
    const anchorRegex = /<a\s+[^>]*href="([^"]+)"[^>]*>([\s\S]*?)<\/a>/g;
    const anchors = [...chunk.matchAll(anchorRegex)];
    if (anchors.length === 0) continue;

    const firstAnchorIdx = chunk.indexOf('<a');
    const prefix =
      firstAnchorIdx > 0
        ? stripTags(chunk.substring(0, firstAnchorIdx)).trim()
        : '';

    for (const a of anchors) {
      const url = a[1].trim();
      const text = stripTags(a[2]).trim();
      const name = prefix ? `${prefix} ${text}` : text;
      items.push({ name, url });
    }
  }
  return items;
}

function stripTags(s) {
  return s.replace(/<[^>]+>/g, '').replace(/\s+/g, ' ');
}

function filterByCategory(rows, keyword) {
  if (!keyword) return rows;
  const tokens = keyword.toLowerCase().split(/\s+/).filter(Boolean);
  if (tokens.length === 0) return rows;
  return rows.filter((r) => {
    const cat = r.category.toLowerCase();
    return tokens.some((t) => cat.includes(t));
  });
}

function buildCategoryBlocks(row) {
  const header = `*:open_file_folder: ${row.category}* (${row.items.length}개)`;
  const lines = row.items.map((i) => `• <${i.url}|${i.name}>`);

  const sections = [];
  let buffer = header;
  for (const line of lines) {
    const candidate = buffer + '\n' + line;
    if (candidate.length > MAX_SECTION_TEXT) {
      sections.push(buffer);
      buffer = line;
    } else {
      buffer = candidate;
    }
  }
  if (buffer) sections.push(buffer);

  return sections.map((text) => ({
    type: 'section',
    text: { type: 'mrkdwn', text },
  }));
}

function buildCopyableSections(filtered) {
  const lines = [];
  for (const row of filtered) {
    if (lines.length > 0) lines.push('');
    lines.push(`[${row.category}]`);
    for (const item of row.items) {
      lines.push(`${item.name}: ${item.url}`);
    }
  }

  const fenceOverhead = '```\n\n```'.length;
  const limit = MAX_SECTION_TEXT - fenceOverhead;
  const sections = [];
  let buffer = [];
  let bufferLen = 0;
  for (const line of lines) {
    const lineLen = line.length + 1;
    if (bufferLen + lineLen > limit && buffer.length > 0) {
      sections.push({
        type: 'section',
        text: { type: 'mrkdwn', text: '```\n' + buffer.join('\n') + '\n```' },
      });
      buffer = [];
      bufferLen = 0;
    }
    buffer.push(line);
    bufferLen += lineLen;
  }
  if (buffer.length > 0) {
    sections.push({
      type: 'section',
      text: { type: 'mrkdwn', text: '```\n' + buffer.join('\n') + '\n```' },
    });
  }
  return sections;
}

async function postToResponseUrl(responseUrl, payload) {
  if (!responseUrl) return;
  await fetch(responseUrl, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  });
}

// ─────────────────────────── Slack 공통 ───────────────────────────

async function computeSlackSignature(secret, timestamp, body) {
  const baseString = `v0:${timestamp}:${body}`;
  const key = await crypto.subtle.importKey(
    'raw',
    new TextEncoder().encode(secret),
    { name: 'HMAC', hash: 'SHA-256' },
    false,
    ['sign']
  );
  const sigBytes = await crypto.subtle.sign(
    'HMAC',
    key,
    new TextEncoder().encode(baseString)
  );
  const hex = Array.from(new Uint8Array(sigBytes))
    .map((b) => b.toString(16).padStart(2, '0'))
    .join('');
  return `v0=${hex}`;
}

function jsonResponse(obj) {
  return new Response(JSON.stringify(obj), {
    headers: { 'Content-Type': 'application/json; charset=utf-8' },
  });
}

function timingSafeEqual(a, b) {
  if (a.length !== b.length) return false;
  let diff = 0;
  for (let i = 0; i < a.length; i++) {
    diff |= a.charCodeAt(i) ^ b.charCodeAt(i);
  }
  return diff === 0;
}
