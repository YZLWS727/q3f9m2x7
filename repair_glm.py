# -*- coding: utf-8 -*-
"""8/19 智谱成品 360 条“AI解析超时暂存”定向补打脚本（GitHub Actions / 本地通用）。

流程：S3 GET 成品 → 解析暂存条目 → 智谱 GLM-4.7-Flash 原厂 API 补打（与生产同提示词）
      → 原地回写标题/情绪/标签 → 本地校验 → S3 PUT + 回读 SHA256 校验。
环境变量：GLM_API_KEY / S3_ENDPOINT / S3_BUCKET / S3_ACCESS_KEY / S3_SECRET_KEY
用法：python repair_glm.py --file "20260819周三全网宏观信息流-智谱.md" [--out-dir ./output] [--dry-run]
"""
import hashlib
import hmac
import json
import os
import random
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone

AI_MODEL = "glm-4.7-flash"
AI_BASE = "https://open.bigmodel.cn/api/paas/v4/chat/completions"
BATCH_SIZE = 10
CALL_TIMEOUT = 120
MAX_BATCH_RETRY = 10
MAX_ITEM_RETRY = 3
PLACEHOLDER = "AI解析超时暂存"

TAGS_DEF = {
    "#中国": '出现"中国"或与中国相关的各类新闻',
    "#美国": '出现"美国"或事件、影响涉及美国',
    "#日本": '出现"日本"或事件、影响涉及日本',
    "#欧洲": '出现"欧洲"或事件、影响涉及欧洲',
    "#亚洲": '出现"亚洲"或事件、影响涉及亚洲',
    "#美洲": '出现"美洲"或事件、影响涉及美洲',
    "#非洲": '出现"非洲"或事件、影响涉及非洲',
    "#其他国家": "上述未覆盖的其他国家相关新闻",
    "#中国央行": "涉及中国央行动作、政策预期、市场影响及相关官员表态的新闻，包含市场对央行后续政策走向的预测",
    "#美联储": "涉及美联储动作、政策预期、市场影响及相关官员表态的新闻，包含市场对美联储后续政策走向的预测",
    "#中国货币政策": "所有与中国货币政策及相互影响的新闻，包含对货币政策的预测、展望与政策走向分析",
    "#中国财政政策": "所有与中国财政政策及相互影响的新闻，包含对财政政策的预测、展望与政策走向分析",
    "#中国地方政府": "所有与中国各省份、各级地方政府相关的新闻，涵盖地方政府主动开展、被动参与及受其影响的，地方政府人事等各类事项",
    "#中国汇率": "所有与人民币汇率及相关政策的新闻",
    "#中国股市": "所有中国相关股票市场的新闻，含A股、港股、B股等全部品类",
    "#金融流动性": "所有与金融流动性相关的新闻，涵盖宏观资金面松紧、市场现金流与资金流向，以及股票、债券、证券等资产的交易流动性与变现能力",
    "#债务和债券市场": "所有与债务及债券市场相关的新闻",
    "#宏观政策": "所有全球各国、地区与宏观经济及宏观调控政策相关的新闻",
    "#期权": "所有与期权市场及品种相关的新闻",
    "#期货": "所有与期货市场及品种相关的新闻",
    "#全球汇率": "所有全球各个国家货币汇率相关问题与走势的新闻",
    "#非中国股市": "所有中国以外国家和地区股票市场的相关新闻",
    "#中国公司信息": "所有中国企业经营动态信息，包含中国公司在海外的子公司、分公司相关动态",
    "#非中国公司信息": "所有与境外企业经营动态相关的新闻",
    "#行业动态": "所有与各行业整体发展相关的新闻。若触发了具体的子行业（如航空业、房地产等），需同时保留此标签",
    "#航空业": "所有与全球航空产业相关的各类新闻",
    "#AI领域": "所有与人工智能技术及应用相关的新闻，属科技领域子集，通常与 科技领域标签同时出现",
    "#科技领域": "所有与广义科技行业相关的各类新闻",
    "#机器人领域": "所有与机器人产业及技术相关的新闻 ，属科技领域子集，通常与 科技领域标签同时出现",
    "#生活科技前沿": "贴近大众日常生活的前沿科技，既包括直接面向消费者的 ToC 科技，也包括通过企业端应用提效、最终传导影响到 C 端生活体验的相关科技，属科技领域子集，通常与 科技领域标签同时出现",
    "#大宗商品": "全球所有与大宗商品市场及价格相关的新闻，包含原油、黄金、农产品等，可与能源、金银等标签共存",
    "#金银": "所有与黄金、白银等贵金属相关的新闻，涵盖价格走势、各国央行操作、供需、持仓、市场观点、社会购买态度、社会层面行为及影响等全部相关内容，只要与黄金白银相关均纳入",
    "#非金银金属": "所有与非贵金属、工业金属相关的新闻",
    "#能源": "所有与能源品类及能源市场相关的新闻",
    "#房地产": "所有与房地产行业及市场相关的新闻",
    "#资产": '新闻中提及"资产"二字，或涉及大类资产配置与价格的相关新闻',
    "#区块链与数字资产": "所有与区块链技术、数字资产相关的新闻，涵盖加密货币、NFT、RWA、分布式账本、Web3、链上应用及监管政策等全部相关内容",
    "#地缘政治": "所有与国际地缘政治博弈相关的新闻",
    "#重要政坛更迭": "各国政坛重要人事变动、换届新闻",
    "#国家政策变化": "全球各国各地区重大政策的预测、出台、主张倾向及具体调整的相关新闻",
    "#突发事件": "突发的灾害、事故、事件等即时新闻",
    "#社会情绪": "能够反映全球各地社会情绪、市场情绪及情绪变化的新闻内容",
    "#精英观点": "知名人士、专家学者公开发表的观点",
    "#趋势预测": "对经济、市场、行业、社会现象、地缘政治等各领域未来走势、发展方向的预判与展望类新闻",
    "#社会重要数据": "全球所有与社会层面重要数据相关的新闻，涵盖宏观与微观层面",
    "#社会消费情况": "所有与居民消费及零售相关的新闻",
    "#生活成本异动": "全球居民生活成本变动、物价异动相关新闻，包含已发生的变动以及对生活成本变化的预测、观点与讨论",
    "#体育或赛事": "所有体育运动、体力、脑力、操作、技能的各种比赛或竞技类活动及赛事相关新闻",
    "#其他自然问题": "气候、地震外的其他自然问题新闻",
    "#气候和天气问题": "所有与气候及极端天气相关的新闻",
    "#地震问题": "所有与地震及次生灾害相关的新闻",
    "#战争分析": "武装冲突战况、影响及分析类新闻",
    "#宏微观对比": "新闻中体现宏观形势与微观感受对照对比思维的分析内容",
    "#本质和信号": "新闻中体现以发掘事件本质、提炼可供市场参考信号为目的的观点、态度与深度思考的分析类内容",
    "#随机性和不对称性": "新闻内容涉及随机性现象、偶然事件以及信息不对称带来的相关问题与讨论，无需出现关键词，内容相关即纳入",
    "#规则理解": "对各类规则的解读内容，涵盖政策制度、市场规则、行业规范以及社会生活各层面的相关规则解析",
    "#祛魅": "体现对事物认知从追捧、神化逐步回归理性的祛魅过程的内容",
    "#典型性偏差": "新闻内容体现出行为金融学中典型性偏差特征的现象或观点，即以特例代表整体、被鲜活案例误导的认知偏差，无需出现关键词，内容相关即纳入",
    "#线性思维": "新闻中体现线性思维模式的内容，包括认为规模与效果呈线性关系、直线外推趋势等认知，涵盖观点、现象与客观事实，相关即纳入",
    "#过度自信": "新闻涉及高估自身判断力、过度自信的认知偏差，包含过度乐观或过度悲观的极端判断，不限于情绪层面，判断层面的过度偏差均纳入",
    "#锚定心态": "新闻体现锚定效应认知心理的内容，即受到先前认知、初始参考值影响而形成判断，或因之前的看法导致对变化感知偏差的相关内容",
    "#模糊规避": "新闻内容反映出模糊厌恶心理的相关现象、事件或观点，即人们倾向回避概率不明的选项、偏好确定性的行为表现，内容相关即纳入",
    "#损失厌恶": "新闻内容反映出损失厌恶心理的现象、事件或观点，即损失带来的痛苦大于同等收益的快乐、怕亏心理主导决策的相关表现，内容相关即纳入",
    "#沉没成本": "新闻内容反映出沉没成本谬误的现象、事件或观点，即因已投入的不可回收成本而继续错误决策的相关表现，内容相关即纳入",
    "#叙事谬误": '新闻内容反映出叙事谬误认知偏差的现象或观点，即为复杂事件强行编造因果故事、过度受叙事误导的相关表现；新闻中直接提及 "叙事" 二字的也一并纳入',
    "#心理账户": "新闻内容反映出心理账户行为经济学现象的相关内容，即人们将资金归入不同心理账户并采取不同决策的行为表现，内容相关即纳入",
    "#货币幻觉": '不仅限于货币层面的名义金额与实际购买力偏差，凡是表面数值与真实实质不相符、人们陷入认知偏差的各类 "幻觉" 现象均纳入，涵盖货币及更广泛的名实不符认知偏差',
    "#人性动机": "新闻内容涉及人性底层动机驱动的相关分析，包括贪婪、恐惧、逐利、从众、自保等人性因素对事件、决策、市场的影响，内容相关即纳入",
    "#教育方面": "涵盖全世界学前至高等教育各阶段学校方面新闻、以及全世界教育界教学政策法规变动，以及全世界教师与学生群体的行为、权益或相关争议事件",
    "#公共卫生与医疗健康": "包含全世界流行病防控、全世界各个国家医保问题，医保药价政策、医疗机构动态、医学科研突破及公众健康安全，如食品药品安全、环境卫生等相关话题",
    "#世界或区域性或地方性组织": "凡涉及主权国家或地区间各类跨国或地方性多边实体或组织,含经济、军事、科技、区域合作等各领域主体动态与相关内容",
    "#娱乐媒体方面": "凡涉及演艺明星、影视综艺、动漫游戏等娱乐产业动态、饭圈文化，以及娱乐媒介、狗仔爆料等主体的相关内容",
    "#无法归类等待识别": "当所有其他标签都不符合时，必须使用此标签",
}
VALID_TAGS = set(TAGS_DEF)


def log(*args):
    print(*args, flush=True)


# ---------------- S3 SigV4 ----------------

def _sign(key, msg):
    return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()


def _s3_request(method, path, body=None, content_type=None):
    endpoint = os.environ["S3_ENDPOINT"].rstrip("/")
    bucket = os.environ["S3_BUCKET"]
    ak = os.environ["S3_ACCESS_KEY"].strip()
    sk = os.environ["S3_SECRET_KEY"].strip()
    region = os.environ.get("S3_REGION", "cn-north-1")
    host = urllib.parse.urlparse(endpoint).netloc
    payload = body or b""
    payload_hash = hashlib.sha256(payload).hexdigest()
    now = datetime.now(timezone.utc)
    amzdate = now.strftime("%Y%m%dT%H%M%SZ")
    datestamp = now.strftime("%Y%m%d")
    headers = {
        "host": host,
        "x-amz-content-sha256": payload_hash,
        "x-amz-date": amzdate,
    }
    if content_type:
        headers["content-type"] = content_type
    canonical_headers = "".join(f"{k}:{v}\n" for k, v in headers.items())
    signed_headers = ";".join(headers.keys())
    canonical_request = "\n".join([method, path, "", canonical_headers, signed_headers, payload_hash])
    scope = f"{datestamp}/{region}/s3/aws4_request"
    sts = "\n".join(["AWS4-HMAC-SHA256", amzdate, scope, hashlib.sha256(canonical_request.encode("utf-8")).hexdigest()])
    kd = _sign(("AWS4" + sk).encode(), datestamp)
    kr = _sign(kd, region)
    ks = _sign(kr, "s3")
    ksg = _sign(ks, "aws4_request")
    sig = hmac.new(ksg, sts.encode("utf-8"), hashlib.sha256).hexdigest()
    auth = f"AWS4-HMAC-SHA256 Credential={ak}/{scope}, SignedHeaders={signed_headers}, Signature={sig}"
    url = endpoint + path
    req = urllib.request.Request(url, data=payload, method=method)
    req.add_header("Authorization", auth)
    for k, v in headers.items():
        req.add_header(k, v)
    req.add_header("User-Agent", "rclone/v1.68.0")
    last = None
    for attempt in range(1, 4):
        try:
            with urllib.request.urlopen(req, timeout=240) as r:
                return r.status, r.read()
        except Exception as e:
            last = repr(e)
            time.sleep(3 * attempt)
    raise RuntimeError("S3 request failed after 3 attempts: %s" % last)


def s3_key_path(key):
    return "/" + os.environ["S3_BUCKET"] + "/" + urllib.parse.quote(key, safe="/")


def s3_get(key):
    st, body = _s3_request("GET", s3_key_path(key))
    if st != 200:
        raise RuntimeError("S3 GET %d" % st)
    return body


def s3_put(key, body):
    st, _ = _s3_request("PUT", s3_key_path(key), body=body, content_type="text/markdown; charset=utf-8")
    if st not in (200, 201):
        raise RuntimeError("S3 PUT %d" % st)


# ---------------- MD 解析 ----------------

def parse_md(text):
    """返回条目列表：{title, meta_line_idx, tags_line_idx, content_lines, block_start}"""
    lines = text.splitlines()
    entries = []
    i = 0
    while i < len(lines):
        m = re.match(r"^### (.+)$", lines[i])
        if not m:
            i += 1
            continue
        title = m.group(1).strip()
        start = i
        i += 1
        while i < len(lines) and not lines[i].strip():
            i += 1
        meta_idx = None
        if i < len(lines) and lines[i].startswith("`"):
            meta_idx = i
            i += 1
        while i < len(lines) and not lines[i].strip():
            i += 1
        tags_idx = None
        if i < len(lines) and lines[i].strip().startswith("#"):
            tags_idx = i
            i += 1
        content = []
        while i < len(lines) and not lines[i].strip().startswith("### ") and lines[i].strip() != "---":
            content.append(lines[i])
            i += 1
        entries.append(
            {
                "title": title,
                "block_start": start,
                "meta_idx": meta_idx,
                "tags_idx": tags_idx,
                "content": "\n".join(content).strip(),
            }
        )
    return entries


def extract_id(meta_line):
    m = re.search(r"\[id::\s*([^\]\s]+)", meta_line or "")
    return m.group(1) if m else None


def extract_time(meta_line):
    m = re.match(r"^`([^`]+)`", meta_line or "")
    return m.group(1).strip() if m else ""


def extract_source_tag(tags_line):
    tags = re.findall(r"#[\u4e00-\u9fffA-Za-z0-9_]+", tags_line or "")
    if tags:
        return tags[0]
    return "#新浪24H"


# ---------------- 智谱补打 ----------------

def build_prompt(batch_data):
    tags_context = ", ".join(f"{tag}(释义:{desc})" for tag, desc in TAGS_DEF.items())
    system_prompt = f"""作为宏观金融与认知心理学专家，请分析 JSON 列表中的快讯并赋予标签。
【核心纪律：绝对白名单制】
你输出的所有标签，必须 100% 存在于下方的《预设标签库》中，一字不差。严禁自创标签。
若无吻合标签，强制且仅输出 ["#无法归类等待识别"]。

【多标签硬性纪律（最高优先级）】
1. 正常情况下每条新闻只输出最相关的 4~6 个标签，宁缺毋滥，不要堆砌弱相关标签；
2. 所有标签必须一字不差来自《预设标签库》，绝对禁止自创、改写、合并或新增任何白名单之外的标签；
3. 除非完全找不到任何匹配，否则每条至少输出 4 个最相关标签；宁可包含中等相关标签，也不要少于 4 个；
4. 标签上限 6 个，绝不超过；
5. 一条都找不到匹配时，只输出 ["#无法归类等待识别"]。

【情绪判定纪律】
sentiment 按新闻事实判断：
1. 有明确利空信号（重大自然灾害致灾/人员伤亡、地缘冲突升级、重大违约/暴跌/衰退等）→ Negative；
2. 有明确利好信号（重大政策利好、显著增长/创新高/评级上调/重大突破等）→ Positive；
3. 无明显利好利空、信息中性或好坏参半 → Neutral（中性优先）；不因主体知名度、行业偏好或叙事语气偏向。

《预设标签库》：
{tags_context}

仅返回纯 JSON 数组，禁止任何 markdown 标记。格式示例：
[ {{"idx": 0, "summary_title": "精炼标题", "tags": ["#匹配到的严格标签1"], "sentiment": "Positive/Negative/Neutral"}} ]"""
    user_payload = [{"idx": i, "text": item["content"]} for i, item in enumerate(batch_data)]
    return system_prompt, json.dumps(user_payload, ensure_ascii=False)


def call_glm(batch_data):
    api_key = os.environ.get("GLM_API_KEY", "").strip()
    system_prompt, user_content = build_prompt(batch_data)
    payload = {
        "model": AI_MODEL,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_content},
        ],
        "thinking": {"type": "disabled"},
        "temperature": 0.0,
    }
    req = urllib.request.Request(
        AI_BASE,
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        headers={"Content-Type": "application/json", "Authorization": "Bearer " + api_key},
        method="POST",
    )
    last_err = None
    for attempt in range(1, MAX_BATCH_RETRY + 1):
        try:
            with urllib.request.urlopen(req, timeout=CALL_TIMEOUT) as r:
                raw = r.read().decode("utf-8")
        except Exception as e:
            last_err = repr(e)
            time.sleep(min(60, 5 * (2 ** (attempt - 1))) + random.uniform(0, 3))
            continue
        try:
            j = json.loads(raw)
        except Exception:
            last_err = "non-json response"
            time.sleep(min(60, 5 * (2 ** (attempt - 1))) + random.uniform(0, 3))
            continue
        if isinstance(j, dict) and j.get("error"):
            code = str(j["error"].get("code", ""))
            if "1214" in code or "modelCode" in str(j["error"]):
                log("MODEL_GONE_ALERT", j["error"])
                return None, True
            if j.get("error", {}).get("message") and any(k in str(j["error"]["message"]) for k in ("鉴权", "认证", "Invalid API key", "Unauthorized")):
                log("GLM_AUTH_FAILED", raw[:200])
                return None, False
            last_err = f"HTTP error {code}: {raw[:150]}"
            time.sleep(min(60, 5 * (2 ** (attempt - 1))) + random.uniform(0, 3))
            continue
        try:
            content = j["choices"][0]["message"]["content"]
            clean = content.replace("```json", "").replace("```", "").strip()
            arr = json.loads(clean)
        except Exception as e:
            last_err = "parse fail: " + repr(e)
            time.sleep(min(60, 5 * (2 ** (attempt - 1))) + random.uniform(0, 3))
            continue
        result = {}
        for r in arr if isinstance(arr, list) else []:
            idx = r.get("idx")
            if not isinstance(idx, int) or not (0 <= idx < len(batch_data)):
                continue
            real_id = batch_data[idx]["id"]
            raw_tags = r.get("tags", []) if isinstance(r.get("tags"), list) else []
            seen = set()
            tags = []
            for t in raw_tags:
                if t in VALID_TAGS and t not in seen:
                    seen.add(t)
                    tags.append(t)
            tags = tags[:6]
            if not tags:
                tags = ["#无法归类等待识别"]
            title = (r.get("summary_title") or "").strip().replace("\n", " ")
            sentiment = r.get("sentiment") if r.get("sentiment") in ("Positive", "Negative", "Neutral") else "Neutral"
            if not title or title == PLACEHOLDER:
                continue
            result[real_id] = {"summary_title": title, "tags": tags, "sentiment": sentiment}
        return result, False
    log("BATCH_FAILED", last_err)
    return {}, False


def label_entries(items):
    """补打 360 条；返回 {id: {summary_title,tags,sentiment}} 与失败列表。"""
    ok = {}
    failed = []
    batches = [items[i : i + BATCH_SIZE] for i in range(0, len(items), BATCH_SIZE)]
    total_batches = len(batches)
    for bi, batch in enumerate(batches, 1):
        res, model_gone = call_glm(batch)
        if model_gone:
            with open("MODEL_GONE_ALERT.txt", "w", encoding="utf-8") as f:
                f.write("智谱模型下线/不存在，修复中止\n")
            log("MODEL_GONE_ABORT batch=%d/%d" % (bi, total_batches))
            sys.exit(2)
        if res is None:
            log("ABORT_AUTH batch=%d/%d" % (bi, total_batches))
            sys.exit(2)
        missing = [it for it in batch if it["id"] not in res]
        for it in missing:
            for attempt in range(1, MAX_ITEM_RETRY + 1):
                single, mg = call_glm([it])
                if mg:
                    sys.exit(2)
                if single and it["id"] in single:
                    res[it["id"]] = single[it["id"]]
                    break
                time.sleep(2 * attempt)
        for it in batch:
            if it["id"] in res:
                ok[it["id"]] = res[it["id"]]
            else:
                failed.append(it["id"])
                log("ITEM_FAILED", it["id"])
        log("BATCH_PROGRESS %d/%d ok=%d fail=%d" % (bi, total_batches, len(ok), len(failed)))
        time.sleep(1.0)
    return ok, failed


# ---------------- 回写 ----------------

def patch_md(text, items, results, failed_ids):
    lines = text.splitlines()
    for it in items:
        if it["id"] in failed_ids:
            continue
        r = results[it["id"]]
        idx = it["block_start"]
        lines[idx] = "### " + r["summary_title"]
        meta_idx = it["meta_idx"]
        if meta_idx is not None:
            lines[meta_idx] = re.sub(
                r"\[sentiment::\s*(?:Positive|Negative|Neutral)\]",
                "[sentiment:: " + r["sentiment"] + "]",
                lines[meta_idx],
            )
        tags_idx = it["tags_idx"]
        if tags_idx is not None:
            src = extract_source_tag(lines[tags_idx])
            new_tags = [src] + [t for t in r["tags"] if t != src]
            lines[tags_idx] = " ".join(new_tags[:6])
    return "\n".join(lines)


def verify(text, expected_total, timeout_ids):
    n_timeout = len(re.findall(r"^### AI解析超时暂存\s*$", text, flags=re.M))
    n_entries = len(re.findall(r"^### ", text, flags=re.M))
    ids = re.findall(r"\[id::\s*([^\]\s]+)", text)
    dup = len(ids) - len(set(ids))
    log("VERIFY placeholders=%d entries=%d dup_ids=%d" % (n_timeout, n_entries, dup))
    return n_timeout == 0 and n_entries == expected_total and dup == 0


# ---------------- 主流程 ----------------

def main():
    args = sys.argv[1:]
    target_file = "20260819周三全网宏观信息流-智谱.md"
    out_dir = "./output"
    dry_run = False
    local_src = None
    i = 0
    while i < len(args):
        if args[i] == "--file" and i + 1 < len(args):
            target_file = args[i + 1]
            i += 2
        elif args[i] == "--out-dir" and i + 1 < len(args):
            out_dir = args[i + 1]
            i += 2
        elif args[i] == "--dry-run":
            dry_run = True
            i += 1
        elif args[i] == "--local-src" and i + 1 < len(args):
            local_src = args[i + 1]
            i += 2
        else:
            i += 1

    if dry_run:
        src = local_src or r"D:\obsidian\OB\新浪24H信息流\20260819周三全网宏观信息流-智谱.md"
        text = open(src, encoding="utf-8").read()
    else:
        log("S3_GET", target_file)
        text = s3_get(target_file).decode("utf-8")

    entries = parse_md(text)
    timeouts = [e for e in entries if e["title"] == PLACEHOLDER]
    log("PARSE total=%d timeout=%d" % (len(entries), len(timeouts)))
    if not timeouts:
        log("NO_TIMEOUT_ENTRIES")
        return 0
    if dry_run:
        empty = sum(1 for e in timeouts if not e["content"])
        log("DRY_RUN ok; empty_content=%d" % empty)
        return 0

    items = []
    for e in timeouts:
        meta = text.splitlines()[e["meta_idx"]] if e["meta_idx"] is not None else ""
        tags_line = text.splitlines()[e["tags_idx"]] if e["tags_idx"] is not None else ""
        items.append(
            {
                "id": extract_id(meta),
                "time": extract_time(meta),
                "source_tag": extract_source_tag(tags_line),
                "content": e["content"],
                "block_start": e["block_start"],
                "meta_idx": e["meta_idx"],
                "tags_idx": e["tags_idx"],
            }
        )
    items = [it for it in items if it["id"]]
    log("TO_RELABEL=%d" % len(items))

    results, failed_ids = label_entries(items)
    log("LABEL_DONE ok=%d fail=%d" % (len(results), len(failed_ids)))

    new_text = patch_md(text, items, results, set(failed_ids))
    os.makedirs(out_dir, exist_ok=True)
    out_md = os.path.join(out_dir, target_file)
    with open(out_md, "w", encoding="utf-8", newline="\n") as f:
        f.write(new_text)

    report = {
        "target_file": target_file,
        "total_entries": len(entries),
        "timeout_entries": len(timeouts),
        "repaired": len(results),
        "failed": failed_ids,
        "entries": [],
    }
    for it in items:
        report["entries"].append(
            {
                "id": it["id"],
                "time": it["time"],
                "source_tag": it["source_tag"],
                "new_title": results.get(it["id"], {}).get("summary_title"),
                "new_tags": results.get(it["id"], {}).get("tags"),
                "new_sentiment": results.get(it["id"], {}).get("sentiment"),
                "failed": it["id"] in failed_ids,
            }
        )
    with open(os.path.join(out_dir, "repair_report.json"), "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=1)

    if not verify(new_text, len(entries), []):
        log("VERIFY_FAILED")
        return 3

    body = new_text.encode("utf-8")
    log("UPLOAD size=%d sha256=%s" % (len(body), hashlib.sha256(body).hexdigest()))
    s3_put(target_file, body)
    back = s3_get(target_file)
    ok_sha = hashlib.sha256(back).hexdigest() == hashlib.sha256(body).hexdigest()
    ok_size = len(back) == len(body)
    log("S3_BACK size=%d sha256=%s MATCH=%s" % (len(back), hashlib.sha256(back).hexdigest(), ok_sha and ok_size))
    if not (ok_sha and ok_size):
        return 3
    if failed_ids:
        log("HAS_FAILED_ENTRIES", failed_ids)
        return 3
    log("REPAIR_DONE all=%d repaired=%d" % (len(items), len(results)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
