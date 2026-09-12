---
title: "Do Document Links Help AI Find What It Needs?"
date: 2026-09-12
draft: false
tags: ["ai", "knowledge-base", "llm", "rag", "data-engineering"]
description: "Can following document links help AI find more relevant documents and use fewer tokens than search? I compared rg search and link navigation using 43 operational documents and 15 questions."
cover:
  image: "/images/linking-knowledge-cover.png"
  alt: "Does linking knowledge help AI navigate"
  relative: false
---

When our team started running an AI agent, one question came up: **How do we give it accurate context about our systems?**

We put Markdown documents in a Git repository and linked related documents together. No vector database, embeddings, or separate search system. The main purpose was to give the AI agent something to read and refer to, but we also wanted the knowledge to be easy for people to find and maintain.

Connecting documents this way, with an AI agent reading them and submitting update PRs, is similar to the [LLM Wiki](https://gist.github.com/karpathy/442a6bf555914893e9891c11519de94f) approach introduced by Andrej Karpathy.

But having links in the documents does not mean the AI uses them to navigate. Our agent mostly finds documents by searching with `rg` and reading the files it needs. What if we let it follow the existing links directly? Would it find the documents it needs more reliably than with search alone? Would it use fewer tokens?

So I compared `rg` search with link navigation using the knowledge base we actually work with.

---

## Experiment Setup

The knowledge base contains **43 Markdown documents** and **285 internal links**. Treat each document as a node and each link as an edge, and you have a graph. The experiment used the actual document contents, but this post only shows the link structure and document titles.

You may be familiar with this kind of linking from Obsidian's `[[document]]` links and graph view. Whether those links improve retrieval is a separate question from whether the graph looks nice.

![Document link graph of the knowledge base](/images/knowledge-link-graph.png)

*The actual knowledge base in Obsidian's graph view. README is the largest hub.*

Each document links to related system and operational documents that should be read together. The links capture relevance, but they do not explicitly label the relationship as "depends on," "sends data to," or "helps investigate a failure in."

The question here is whether **the existing links in our Markdown knowledge base help AI find documents**. I wanted to see whether the AI could use the connections people had made between related documents.

The test was straightforward. I wrote 15 questions asking the AI to find documents on a topic and defined a **reference set of relevant documents** for each question in advance. The questions covered broad topics, specific tasks, topics requiring both an overview and detailed documents, and questions phrased differently from document names.

| Question type | Examples | Reference documents |
|---|---|---|
| Broad topics (5) | Onboarding / PII and security / CDC and streaming / Jira / Data warehouse | 2–4 per question |
| Specific tasks (4) | "VPN is not working" / "Investigate unexpected dashboard data" / "AWS CLI access" / "Jenkins build problems" | 1–2 per question |
| Overview + details (3) | "Overall architecture" / "Tech stack and repository structure" / "How data flows from ingestion to storage" | Overview + details, 2–3 documents |
| Keyword mismatch (3) | "Handle personal data requests" / "Run code remotely on a server" / "Where does marketing data come from?" | 1–3 per question |

Here, "found" means the documents the AI selected at the end of its exploration, rather than every document it read along the way. I used two metrics.

- **Recall**: How many of the reference documents did it find? `reference documents selected / total reference documents`
- **Precision**: How many of its final selections were reference documents? `reference documents selected / total documents selected`

I chose the reference documents based only on the topic and the actual contents, independently of the link structure. Using the links to define the answers would make the test circular: links would be good at finding linked documents.

I ran the experiment through the OpenAI API.

---

## Letting the AI Explore Like Our Agent Does

Our team's OpenClaw-based AI agent searches a knowledge repository mounted on its filesystem, alternating between `rg` searches and reading Markdown files as it explores. I set up the experiment to resemble that workflow and compared what happened when link navigation was available.

All three conditions started by reading README. After that, the AI chose its next action from the tools available in each condition.

| Condition | Available tools |
|---|---|
| Search | Search with `rg`, then `read` the documents it needs |
| Links | List links in the current document, then `read` the documents it needs |
| Both | `rg`, link listing, and `read` |

The links inside the documents remained intact in all three conditions. The comparison was about providing search and a dedicated link navigation tool, separately or together, rather than adding or removing links from the documents.

I limited `rg` results to eight files, three lines per file, and 120 characters per line to keep the search output from getting too long. The link listing returned both the link text and the target path. The agent loop allowed up to 15 steps and eight document reads, excluding the initial README read.

Scoring used the final document set submitted through `done(selected_files)`. If a run reached the limit without calling `done`, the documents read up to that point were treated as its final selection.

I ran four models under all three conditions, repeating each combination three times. That made 36 runs across the question set, or 540 individual question runs. Recall below is the mean and standard deviation across the three repetitions. Precision and tokens are means.

For tokens, I summed the input prompt tokens across all API calls needed to answer one question, then averaged that total over the 15 questions and three repetitions. Model output tokens are not included.

| Model | Condition | Recall | Precision | Input tokens |
|---|---|---:|---:|---:|
| `gpt-4o-mini` | Search | 0.68 ± 0.06 | 0.36 | 16.0k |
| | Links | **0.80 ± 0.02** | 0.38 | 22.5k |
| | Both | 0.79 ± 0.04 | 0.41 | 15.1k |
| `gpt-4o` | Search | 0.73 ± 0.04 | 0.51 | 13.5k |
| | Links | 0.74 ± 0.03 | 0.56 | 13.1k |
| | Both | **0.79 ± 0.06** | 0.45 | 21.4k |
| `gpt-5.4-mini` | Search | 0.85 ± 0.03 | 0.35 | 17.4k |
| | Links | **0.86 ± 0.02** | 0.38 | 14.2k |
| | Both | 0.84 ± 0.01 | 0.31 | 18.1k |
| `gpt-5.5` | Search | 0.97 ± 0.01 | 0.38 | 46.9k |
| | Links | 0.90 ± 0.01 | 0.37 | 23.1k |
| | Both | **0.99 ± 0.02** | 0.38 | 37.8k |

(Bold values mark the condition with the highest recall for each model.)

The pattern differed by model. The clearest difference was in `gpt-4o-mini`: recall rose from 0.68 with search to 0.80 with links. The differences were smaller for `gpt-4o` and `gpt-5.4-mini`. These patterns appeared across three repetitions, but that is not enough to draw a general rule about model size.

For `gpt-5.5`, search achieved higher recall than links: 0.97 versus 0.90. It called `rg` more than eight times per question, searching broadly for the documents it needed.

With both tools available, recall was higher than search alone for three models and 0.01 lower for `gpt-5.4-mini`. In this condition, `gpt-5.5` made 5.2 `rg` calls and 2.3 link listing calls per question. It actually used the links when given access to both tools. Token usage was less consistent. Links used the fewest tokens for `gpt-5.5`, but also found fewer reference documents. Providing both tools increased token usage for some models and reduced it for others.

Precision ranged from 0.31 to 0.56, below recall. For example, `gpt-5.5` achieved 0.99 recall with both tools, but its precision was 0.38. It selected almost every reference document, along with quite a few documents outside the reference set. This experiment measured document retrieval and selection. It did not evaluate the accuracy of answers generated from those documents.

---

## Did Document Links Help?

In some cases, yes. When the existing links were available as a way to navigate, some models found more reference documents than they did with search alone. But the effect varied by model, and providing both search and links did not always improve results. Input token usage did not consistently decrease either.

This experiment did not compare documents with and without links. All conditions used the same source documents. What changed was whether the AI had a tool for navigating the existing links. These results therefore do not show that document links are unnecessary or that adding more links would have no benefit.

My practical takeaway is narrower. **For now, I have not seen enough reason to spend more time on links specifically for AI retrieval.** I will keep the links that help people find and maintain documents, but I do not plan to spend time adding links solely to improve AI performance.

---
