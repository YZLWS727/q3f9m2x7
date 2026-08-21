# -*- coding: utf-8 -*-
"""a3 打标（GitHub Actions / 本地通用）：硅基流动 Qwen3-8B(强提示词)+GLM-4-9B 分歧检测
自包含：标签库、成品格式、S3 SigV4、检查点云同步全部在本文件。
环境变量：SILICONFLOW_API_KEY / RAW_PATH / TARGET_DATE / OUT_DIR / TIME_BUDGET_MIN
          S3_ENDPOINT / S3_BUCKET / S3_ACCESS_KEY / S3_SECRET_KEY（可 A3_SKIP_S3=1 跳过上传）
注意：TAGS_DEF 与生产脚本同步维护；a2 正在使用智谱，本脚本不调用智谱/DeepSeek。
"""
import hashlib
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

BASE = "https://api.siliconflow.cn/v1"
BATCH_SIZE = 10
CALL_TIMEOUT = 180
MAX_RETRIES = 4
DEFAULT_TIME_BUDGET_MIN = 330
NO_PROGRESS_MIN = 30
MODEL_QWEN3 = "Qwen/Qwen3-8B"
MODEL_GLM9B = "THUDM/GLM-4-9B-0414"

TAGS_DEF = {
    "#中国": "出现\"中国\"或与中国相关的各类新闻",
    "#美国": "出现\"美国\"或事件、影响涉及美国",
    "#日本": "出现\"日本\"或事件、影响涉及日本",
    "#欧洲": "出现\"欧洲\"或事件、影响涉及欧洲",
    "#亚洲": "出现\"亚洲\"或事件、影响涉及亚洲",
    "#美洲": "出现\"美洲\"或事件、影响涉及美洲",
    "#非洲": "出现\"非洲\"或事件、影响涉及非洲",
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
    "#资产": "新闻中提及\"资产\"二字，或涉及大类资产配置与价格的相关新闻",
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
    "#叙事谬误": "新闻内容反映出叙事谬误认知偏差的现象或观点，即为复杂事件强行编造因果故事、过度受叙事误导的相关表现；新闻中直接提及 \"叙事\" 二字的也一并纳入",
    "#心理账户": "新闻内容反映出心理账户行为经济学现象的相关内容，即人们将资金归入不同心理账户并采取不同决策的行为表现，内容相关即纳入",
    "#货币幻觉": "不仅限于货币层面的名义金额与实际购买力偏差，凡是表面数值与真实实质不相符、人们陷入认知偏差的各类 \"幻觉\" 现象均纳入，涵盖货币及更广泛的名实不符认知偏差",
    "#人性动机": "新闻内容涉及人性底层动机驱动的相关分析，包括贪婪、恐惧、逐利、从众、自保等人性因素对事件、决策、市场的影响，内容相关即纳入",
    "#教育方面": "涵盖全世界学前至高等教育各阶段学校方面新闻、以及全世界教育界教学政策法规变动，以及全世界教师与学生群体的行为、权益或相关争议事件",
    "#公共卫生与医疗健康": "包含全世界流行病防控、全世界各个国家医保问题，医保药价政策、医疗机构动态、医学科研突破及公众健康安全，如食品药品安全、环境卫生等相关话题",
    "#世界或区域性或地方性组织": "凡涉及主权国家或地区间各类跨国或地方性多边实体或组织,含经济、军事、科技、区域合作等各领域主体动态与相关内容",
    "#娱乐媒体方面": "凡涉及演艺明星、影视综艺、动漫游戏等娱乐产业动态、饭圈文化，以及娱乐媒介、狗仔爆料等主体的相关内容",
    "#无法归类等待识别": "当所有其他标签都不符合时，必须使用此标签",
}
VALID_TAGS_SET = set(TAGS_DEF.keys())


def log(*args):
    print(*args, flush=True)


def parse_raw_all(path, limit=None):
    text = open(path, encoding="utf-8").read()
    blocks = re.split(r"\n(?=### \[)", text)
    items = []
    for b in blocks:
        m = re.match(r"### \[(#.+?)\] (\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})", b)
        if not m:
            continue
        tag, ct = m.group(1), m.group(2)
        rich = re.sub(r"^### .*?\n\n", "", b, count=1)
        rich = re.split(r"\n+---[ \t]*\n*$", rich)[0].strip()
        if not rich:
            continue
        dt = ct.replace("-", "").replace(":", "").replace(" ", "")
        h = hashlib.md5(rich.encode("utf-8")).hexdigest()[:5]
        items.append({"id": f"{dt}_{h}", "create_time": ct, "source_tag": tag, "rich_text": rich})
    if limit:
        items = items[:limit]
    return items


def date_tag_from_path(path, target_date):
    base = os.path.basename(path)
    m = re.search(r"(\d{8})(周[一二三四五六日天])", base)
    if m and m.group(1) == target_date:
        return m.group(1) + m.group(2)
    raise RuntimeError(f"DATE_TAG_NOT_MATCH path={base} target={target_date}")


def build_prompt(batch):
    tags_context_list = []
    for tag, desc in TAGS_DEF.items():
        if desc.strip():
            tags_context_list.append(f"{tag}(释义:{desc})")
        else:
            tags_context_list.append(tag)
    tags_context_str = ", ".join(tags_context_list)
    system_prompt = f"""作为宏观金融与认知心理学专家，请分析 JSON 列表中的快讯并赋予标签。
【核心纪律：绝对白名单制】
你输出的所有标签，必须 100% 存在于下方的《预设标签库》中，一字不差。严禁自创标签。
若无吻合标签，强制且仅输出 ["#无法归类等待识别"]。

【多标签硬性纪律（最高优先级）】
1. 正常情况下每条新闻只输出最相关的 4~6 个标签，宁缺毋滥，不要堆砌弱相关标签；
2. 所有标签必须一字不差来自《预设标签库》，绝对禁止自创、改写、合并或新增任何白名单之外的标签；
3. 每条必须输出 4~6 个标签：即使最相关标签不足 4 个，也必须从《预设标签库》中选择中等相关但合理的标签补齐，严禁少于 4 个；
4. 标签上限 6 个，绝不超过；
5. 一条都找不到匹配时，只输出 ["#无法归类等待识别"]；
6. 输出前逐条自检：每条标签数必须在 4~6 之间，不足 4 个立即补足后再输出。

【情绪判定纪律】
sentiment 按新闻事实判断：
1. 有明确利空信号（重大自然灾害致灾/人员伤亡、地缘冲突升级、重大违约/暴跌/衰退等）→ Negative；
2. 有明确利好信号（重大政策利好、显著增长/创新高/评级上调/重大突破等）→ Positive；
3. 无明显利好利空、信息中性或好坏参半 → Neutral（中性优先）；不因主体知名度、行业偏好或叙事语气偏向。

《预设标签库》：
{tags_context_str}

仅返回纯 JSON 数组，禁止任何 markdown 标记。格式示例：
[ {{"idx": 0, "summary_title": "精炼标题", "tags": ["#美国", "#债务和债券市场", "#金融流动性", "#宏观政策"], "sentiment": "Neutral"}} ]"""
    user_payload = [{"idx": i, "text": item["rich_text"]} for i, item in enumerate(batch)]
    return system_prompt, json.dumps(user_payload, ensure_ascii=False)


def call_api(model, key, sys_prompt, user_content, extra, counters):
    payload = {"model": model,
               "messages": [{"role": "system", "content": sys_prompt},
                            {"role": "user", "content": user_content}],
               "temperature": 0.0}
    if extra:
        payload.update(extra)
    req = urllib.request.Request(BASE + "/chat/completions", method="POST")
    req.add_header("Authorization", "Bearer " + key)
    req.add_header("Content-Type", "application/json")
    req.add_header("Accept", "application/json")
    data = json.dumps(payload).encode("utf-8")
    last = None
    for attempt in range(1, MAX_RETRIES + 1):
        t0 = time.time()
        try:
            with urllib.request.urlopen(req, data=data, timeout=CALL_TIMEOUT) as r:
                body = r.read().decode("utf-8", "replace")
                counters["ok"] += 1
                return 200, body, time.time() - t0
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", "replace")[:300]
            last = (e.code, body)
            counters["http"][str(e.code)] = counters["http"].get(str(e.code), 0) + 1
            if e.code in (429, 500, 502, 503, 504):
                time.sleep(min(60, 3 * (2 ** (attempt - 1))) + (attempt * 0.7))
                continue
            return e.code, body, time.time() - t0
        except Exception as e:
            last = (-1, repr(e)[:200])
            counters["net"] += 1
            time.sleep(min(60, 3 * (2 ** (attempt - 1))) + (attempt * 0.7))
    return last[0], last[1], 0


def parse_result(body, batch_len):
    try:
        content = json.loads(body)["choices"][0]["message"]["content"]
    except Exception:
        return None, "NO_JSON"
    clean = content.replace("```json", "").replace("```", "").strip()
    try:
        arr = json.loads(clean)
    except Exception:
        return None, "PARSE_FAIL " + clean[:120]
    res = {}
    for r in arr:
        idx = r.get("idx")
        if not isinstance(idx, int) or not (0 <= idx < batch_len):
            continue
        raw_tags = r.get("tags", [])
        if not isinstance(raw_tags, list):
            raw_tags = []
        seen = set()
        tags = []
        for t in raw_tags:
            if t in VALID_TAGS_SET and t not in seen:
                seen.add(t)
                tags.append(t)
        tags = tags[:6]
        if not tags:
            tags = ["#无法归类等待识别"]
        r["tags"] = tags
        res[idx] = r
    missing = [i for i in range(batch_len) if i not in res]
    return res, None if not missing else f"MISSING_IDX {missing}"


def item_ok(r):
    return bool(r) and r.get("tags") != ["#无法归类等待识别"] and \
        r.get("summary_title", "").strip() and r.get("sentiment") in ("Positive", "Negative", "Neutral")


def try_model(model, extra, key, batch, counters):
    sys_prompt, user_content = build_prompt(batch)
    st, body, dt = call_api(model, key, sys_prompt, user_content, extra, counters)
    if st != 200:
        return None, {"http": st, "latency": round(dt, 1), "err": body[:200]}
    res, err = parse_result(body, len(batch))
    if err:
        time.sleep(1)
        st2, body2, dt2 = call_api(model, key, sys_prompt, user_content, extra, counters)
        if st2 == 200:
            res, err = parse_result(body2, len(batch))
            dt += dt2
    return res, {"http": st, "latency": round(dt, 1), "err": err}


def process_batch(bi, batch, key, counters):
    ids = [it["id"] for it in batch]
    res_q, meta_q = try_model(MODEL_QWEN3, {"enable_thinking": False}, key, batch, counters)
    res_g, meta_g = try_model(MODEL_GLM9B, None, key, batch, counters)
    merged = {}
    for i, it in enumerate(batch):
        q, g = (res_q or {}).get(i), (res_g or {}).get(i)
        chosen, src = None, None
        if item_ok(q):
            chosen, src = q, MODEL_QWEN3
        elif item_ok(g):
            chosen, src = g, MODEL_GLM9B
        if chosen:
            merged[it["id"]] = dict(chosen)
            merged[it["id"]]["_model"] = src

    missing_ids = [it["id"] for it in batch if it["id"] not in merged]
    if missing_ids:
        log(f"BATCH {bi} SINGLE_ITEM_FALLBACK n={len(missing_ids)}")
        for i, it in enumerate(batch):
            if it["id"] in merged:
                continue
            sys_prompt, user_content = build_prompt([it])
            for _ in range(2):
                st, body, dt = call_api(MODEL_QWEN3, key, sys_prompt, user_content,
                                        {"enable_thinking": False}, counters)
                if st == 200:
                    r2, err2 = parse_result(body, 1)
                    if r2 and 0 in r2 and item_ok(r2[0]):
                        merged[it["id"]] = dict(r2[0])
                        merged[it["id"]]["_model"] = MODEL_QWEN3 + "-single"
                        break
                time.sleep(1)

    dead = [iid for iid in ids if iid not in merged]
    agree = {"both": 0, "tags_exact": 0, "sentiment": 0, "jaccard_sum": 0.0, "disagree": []}
    for i in range(len(batch)):
        q, g = (res_q or {}).get(i), (res_g or {}).get(i)
        if q and g:
            agree["both"] += 1
            a, b = set(q["tags"]), set(g["tags"])
            if a == b:
                agree["tags_exact"] += 1
            j = len(a & b) / max(1, len(a | b))
            agree["jaccard_sum"] += j
            if q.get("sentiment") == g.get("sentiment"):
                agree["sentiment"] += 1
            if q.get("sentiment") != g.get("sentiment") or j < 0.3:
                agree["disagree"].append(batch[i]["id"])
    return {"batch": bi, "items": ids,
            "qwen_items": {str(i): r for i, r in (res_q or {}).items()},
            "glm9b_items": {str(i): r for i, r in (res_g or {}).items()},
            "merged": merged, "dead": dead, "qwen_meta": meta_q, "glm_meta": meta_g,
            "agree": {k: (round(v, 3) if isinstance(v, float) else v) for k, v in agree.items()},
            "done": True}


def save_json(path, obj):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=1)
    os.replace(tmp, path)


def format_markdown_item(news_id, item, ai_res):
    tags_list = ai_res.get("tags", [])
    if not isinstance(tags_list, list) or not tags_list:
        tags_list = ["#无法归类等待识别"]
    source_tag = item.get("source_tag", "#新浪24H")
    if source_tag not in tags_list:
        tags_list.insert(0, source_tag)
    summary_title = ai_res.get("summary_title", "AI处理异常缺省标题")
    sentiment = ai_res.get("sentiment", "Neutral")
    md_text = f"### {summary_title}\n"
    md_text += f"`{item.get('create_time', '')}` ｜ [id:: {news_id}] ｜ [sentiment:: {sentiment}]  \n"
    md_text += " ".join(tags_list) + "\n\n"
    md_text += item.get("rich_text", "") + "\n\n---\n\n"
    return md_text


def extract_stats_block(path):
    try:
        text = open(path, encoding="utf-8").read()
    except OSError:
        return ""
    m = re.search(r"^> \*\*📊 数据源统计\*\*.*?(?=\n### )", text, re.S | re.M)
    return m.group(0) + "\n" if m else ""


def build_final_md(items, result_by_id, stats_block, date_tag):
    header = f"# {date_tag[:4]}-{date_tag[4:6]}-{date_tag[6:8]} {date_tag[8:]} 宏观信息流\n\n"
    if stats_block:
        header += stats_block + "\n"
    blocks = []
    for it in items:
        ai_res = result_by_id.get(it["id"], {})
        if not ai_res:
            ai_res = {"summary_title": "AI解析遗漏暂存", "tags": ["#无法归类等待识别"], "sentiment": "Neutral"}
        txt = re.sub(r"\n+---[ \t]*\n*$", "", format_markdown_item(it["id"], it, ai_res).strip())
        blocks.append(txt)
    return header.rstrip("\n") + "\n\n" + "\n\n---\n\n".join(blocks) + "\n\n---\n\n"


def s3_cfg():
    return {"endpoint": os.environ.get("S3_ENDPOINT", "").rstrip("/"),
            "bucket": os.environ.get("S3_BUCKET", ""),
            "access_key": os.environ.get("S3_ACCESS_KEY", ""),
            "secret_key": os.environ.get("S3_SECRET_KEY", ""),
            "region": os.environ.get("S3_REGION", "cn-north-1")}


def s3_request(method, key, body=None, query="", content_type=None, cfg=None):
    cfg = cfg or s3_cfg()
    endpoint = cfg["endpoint"]
    bucket = cfg["bucket"]
    host = urllib.parse.urlparse(endpoint).netloc
    path = "/" + bucket + (("/" + urllib.request.quote(key, safe="/")) if key else "")
    now = time.gmtime()
    amzdate = time.strftime("%Y%m%dT%H%M%SZ", now)
    datestamp = time.strftime("%Y%m%d", now)
    payload = body or b""
    payload_hash = hashlib.sha256(payload).hexdigest()
    canonical_headers = f"host:{host}\nx-amz-content-sha256:{payload_hash}\nx-amz-date:{amzdate}\n"
    signed_headers = "host;x-amz-content-sha256;x-amz-date"
    canonical_request = "\n".join([method, path, query, canonical_headers, signed_headers, payload_hash])
    scope = f"{datestamp}/{cfg['region']}/s3/aws4_request"
    sts = "\n".join(["AWS4-HMAC-SHA256", amzdate, scope,
                     hashlib.sha256(canonical_request.encode("utf-8")).hexdigest()])

    def sign(k, msg):
        h = hashlib.new("sha256", k)
        h.update(msg.encode("utf-8"))
        return h.digest()

    kd = sign(("AWS4" + cfg["secret_key"]).encode(), datestamp)
    kr = sign(kd, cfg["region"])
    ks = sign(kr, "s3")
    ksg = sign(ks, "aws4_request")
    sig = hashlib.new("sha256", ksg)
    sig.update(sts.encode("utf-8"))
    auth = (f"AWS4-HMAC-SHA256 Credential={cfg['access_key']}/{scope}, "
            f"SignedHeaders={signed_headers}, Signature={sig.hexdigest()}")
    url = f"{endpoint}{path}" + (f"?{query}" if query else "")
    req = urllib.request.Request(url, data=payload, method=method)
    req.add_header("Authorization", auth)
    req.add_header("x-amz-date", amzdate)
    req.add_header("x-amz-content-sha256", payload_hash)
    req.add_header("Host", host)
    req.add_header("User-Agent", "rclone/v1.68.0")
    if content_type:
        req.add_header("Content-Type", content_type)
    with urllib.request.urlopen(req, timeout=180) as r:
        return r.status, r.read()


def s3_put_retry(key, body, content_type, tries=4, wait=10):
    last = None
    for i in range(1, tries + 1):
        try:
            s3_request("PUT", key, body=body, content_type=content_type)
            return True
        except Exception as e:
            last = e
            log(f"S3_PUT_RETRY {key} {i}/{tries} {e!r}")
            time.sleep(wait)
    raise last


def s3_get_optional(key):
    try:
        _, data = s3_request("GET", key)
        return data
    except urllib.error.HTTPError as e:
        if e.code in (404, 403):
            return None
        raise
    except Exception:
        return None


def main():
    args = sys.argv[1:]
    limit = None
    fresh = False
    time_budget_min = int(os.environ.get("TIME_BUDGET_MIN", str(DEFAULT_TIME_BUDGET_MIN)))
    out_dir = os.environ.get("OUT_DIR", "./output-a3")
    skip_s3 = os.environ.get("A3_SKIP_S3", "0") == "1"
    i = 0
    while i < len(args):
        if args[i] == "--limit" and i + 1 < len(args):
            limit = int(args[i + 1]); i += 2
        elif args[i] == "--time-budget-min" and i + 1 < len(args):
            time_budget_min = int(args[i + 1]); i += 2
        elif args[i] == "--fresh":
            fresh = True; i += 1
        elif args[i] == "--out-dir" and i + 1 < len(args):
            out_dir = args[i + 1]; i += 2
        else:
            i += 1

    raw_path = os.environ.get("RAW_PATH", "")
    target_date = os.environ.get("TARGET_DATE", "")
    key = os.environ.get("SILICONFLOW_API_KEY", "")
    if not raw_path or not os.path.exists(raw_path):
        log("RAW_PATH_MISSING", raw_path)
        return 2
    if not key:
        log("SILICONFLOW_API_KEY_MISSING")
        return 2
    if not target_date:
        m = re.search(r"(\d{8})", os.path.basename(raw_path))
        target_date = m.group(1) if m else ""
    if not target_date:
        log("TARGET_DATE_MISSING")
        return 2

    date_tag = date_tag_from_path(raw_path, target_date)
    os.makedirs(out_dir, exist_ok=True)
    ckpt_path = os.path.join(out_dir, "checkpoint.json")
    s3_ckpt_key = f"checkpoint-a3/{date_tag}-ckpt.json"

    if not fresh:
        if not skip_s3:
            data = s3_get_optional(s3_ckpt_key)
            if data:
                with open(ckpt_path, "wb") as f:
                    f.write(data)
                log("CKPT_DOWNLOADED_FROM_S3", s3_ckpt_key, len(data))

    items = parse_raw_all(raw_path, limit)
    sha = hashlib.sha256()
    with open(raw_path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            sha.update(chunk)
    cfg = {"raw_sha": sha.hexdigest()[:16], "limit": limit, "prompt": "strong",
           "batch_size": BATCH_SIZE, "models": [MODEL_QWEN3, MODEL_GLM9B],
           "date_tag": date_tag, "total": len(items)}
    total_batches = (len(items) + BATCH_SIZE - 1) // BATCH_SIZE

    cp = None
    if not fresh and os.path.exists(ckpt_path):
        try:
            cp = json.load(open(ckpt_path, encoding="utf-8"))
            if cp.get("config") != cfg:
                log("CONFIG_MISMATCH -> FRESH")
                cp = None
            else:
                log("RESUME_LOCAL", cp.get("done_count", 0), "/", total_batches)
        except Exception:
            cp = None
    if cp is None:
        cp = {"config": cfg, "batches": {}, "done_count": 0,
              "started_at": time.strftime("%Y-%m-%d %H:%M:%S"), "updated_at": ""}
        save_json(ckpt_path, cp)

    start_all = time.time()
    last_progress = time.time()
    counters = {"ok": 0, "net": 0, "http": {}}
    merged_all = {}
    dead_all = {}
    disagree_all = []
    exit_code = 0
    batches = [items[i:i + BATCH_SIZE] for i in range(0, len(items), BATCH_SIZE)]

    for bi, batch in enumerate(batches, 1):
        if cp["batches"].get(str(bi), {}).get("done"):
            rec = cp["batches"][str(bi)]
            merged_all.update(rec.get("merged", {}))
            for iid in rec.get("dead", []):
                dead_all[iid] = {"batch": bi, "reason": "resume"}
            disagree_all.extend(rec.get("agree", {}).get("disagree", []))
            continue
        if time.time() - start_all > time_budget_min * 60:
            log("TIME_BUDGET_HIT", "done", cp["done_count"], "/", total_batches)
            exit_code = 3
            break
        if time.time() - last_progress > NO_PROGRESS_MIN * 60:
            log("NO_PROGRESS_HIT", "last_done", cp["done_count"])
            exit_code = 4
            break

        log(f"BATCH {bi}/{total_batches} START elapsed={int(time.time()-start_all)}s")
        rec = process_batch(bi, batch, key, counters)
        cp["batches"][str(bi)] = rec
        cp["done_count"] += 1
        cp["updated_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
        save_json(ckpt_path, cp)
        last_progress = time.time()
        merged_all.update(rec["merged"])
        for iid in rec["dead"]:
            dead_all[iid] = {"batch": bi, "reason": "failed"}
        disagree_all.extend(rec["agree"]["disagree"])
        if cp["done_count"] % 25 == 0 and not skip_s3:
            s3_put_retry(s3_ckpt_key, open(ckpt_path, "rb").read(), "application/octet-stream")
        eta = (time.time() - start_all) / max(1, cp["done_count"]) * (total_batches - cp["done_count"])
        with open(os.path.join(out_dir, "progress.txt"), "w", encoding="utf-8") as f:
            f.write(f"done={cp['done_count']}/{total_batches} eta_sec={int(eta)}\n")
        log(f"BATCH {bi} DONE merged={len(rec['merged'])} dead={len(rec['dead'])} "
            f"disagree={len(rec['agree']['disagree'])} qwen={rec['qwen_meta']['http']} "
            f"glm={rec['glm_meta']['http']} elapsed={int(time.time()-start_all)}s")

    if exit_code == 0 and cp["done_count"] < total_batches:
        exit_code = 3

    if not skip_s3:
        try:
            s3_put_retry(s3_ckpt_key, open(ckpt_path, "rb").read(), "application/octet-stream")
        except Exception as e:
            log("CKPT_S3_UPLOAD_FAIL", repr(e))

    if exit_code == 0:
        result_by_id = dict(merged_all)
        for iid in dead_all:
            result_by_id.setdefault(iid, {})
        stats_block = extract_stats_block(raw_path)
        md = build_final_md(items, result_by_id, stats_block, date_tag)
        md_name = f"{date_tag}全网宏观信息流-硅基流动.md"
        md_path = os.path.join(out_dir, md_name)
        with open(md_path, "w", encoding="utf-8", newline="\n") as f:
            f.write(md)
        save_json(os.path.join(out_dir, "final_results.json"), {
            "total": len(items), "merged": len(merged_all), "dead": dead_all,
            "disagree": disagree_all, "counters": counters,
            "done_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "elapsed_sec": int(time.time() - start_all)})
        save_json(os.path.join(out_dir, "disagreement.json"), disagree_all)
        save_json(os.path.join(out_dir, "dead_letters.json"), dead_all)
        log("FINAL_DONE", md_path, "items", len(items), "merged", len(merged_all),
            "dead", len(dead_all), "disagree", len(disagree_all), "elapsed", int(time.time() - start_all))
        if not skip_s3:
            md_bytes = open(md_path, "rb").read()
            s3_put_retry(md_name, md_bytes, "text/markdown; charset=utf-8")
            try:
                _, got = s3_request("GET", md_name)
                if hashlib.sha256(got).hexdigest() == hashlib.sha256(md_bytes).hexdigest():
                    log("S3_UPLOAD_VERIFIED", md_name, len(got))
                else:
                    log("S3_UPLOAD_HASH_MISMATCH", md_name)
            except Exception as e:
                log("S3_VERIFY_FAIL", repr(e))
            s3_put_retry(f"a3-reports/{date_tag}/disagreement.json",
                         json.dumps(disagree_all, ensure_ascii=False).encode("utf-8"),
                         "application/json")
            s3_put_retry(f"a3-reports/{date_tag}/dead_letters.json",
                         json.dumps(dead_all, ensure_ascii=False).encode("utf-8"),
                         "application/json")
    else:
        log("RESUME_LATER exit", exit_code, "done", cp["done_count"], "/", total_batches)
    log("COUNTERS", json.dumps(counters, ensure_ascii=False))
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
