"use strict";

const fields = ["time-input", "context-input", "avoid-input", "wish-input"];
const labels = ["쓸 수 있는 시간", "장소와 상황", "피하고 싶은 것", "원하는 느낌"];
const output = document.getElementById("context-prompt");

function updatePrompt() {
  if (!output) return;
  const lines = fields.map((id, index) => {
    const value = document.getElementById(id).value.trim();
    return value ? `- ${labels[index]}: ${value}` : null;
  }).filter(Boolean);
  output.textContent = [
    lines.length ? "내 상황을 더 알려줄게." : "집에서 잠깐 할 수 있는 활동을 찾고 있어.",
    ...lines,
    "",
    lines.length ? "이 조건에 맞는 활동 세 가지를 제안해줘." : "활동 세 가지를 제안해줘.",
    "각각 첫 행동과 예상 시간을 짧게 적어줘.",
    "필요한 물건이 있다면 따로 적고, 내가 갖고 있다고 가정하지 마."
  ].join("\n");
}

fields.forEach(id => document.getElementById(id)?.addEventListener("input", updatePrompt));

document.querySelectorAll("[data-copy]").forEach(button => {
  button.addEventListener("click", async () => {
    const source = document.getElementById(button.dataset.copy);
    if (!source) return;
    const copyText = 'value' in source ? source.value : source.textContent;
    const originalLabel = button.textContent;
    let copied = false;
    try {
      if (navigator.clipboard?.writeText) {
        await navigator.clipboard.writeText(copyText);
        copied = true;
      }
    } catch (_) { /* Local files may not allow the Clipboard API. */ }
    if (!copied) {
      const selection = window.getSelection();
      if (source instanceof HTMLTextAreaElement || source instanceof HTMLInputElement) {
        source.select();
      } else {
        const range = document.createRange();
        range.selectNodeContents(source);
        selection.removeAllRanges();
        selection.addRange(range);
      }
      try { copied = document.execCommand("copy"); } catch (_) { copied = false; }
      if (copied) selection.removeAllRanges();
    }
    let status = button.parentElement.querySelector(".copy-message");
    if (!status) {
      status = document.createElement("p");
      status.className = "status copy-message";
      status.setAttribute("role", "status");
      button.insertAdjacentElement("afterend", status);
    }
    status.textContent = copied
      ? "복사했어요. AI 대화창에 붙여 넣어 보내주세요."
      : "문장을 선택했어요. ⌘C 또는 Ctrl+C로 복사해주세요.";
    button.textContent = copied ? "복사했어요 ✓" : "직접 복사해주세요";
    window.setTimeout(() => { button.textContent = originalLabel; }, 2500);
  });
});

const feedback = {
  context: {
    a: "다시 요청하면 바뀔 수는 있지만, 무엇이 안 맞는지는 알기 어려워요. ‘준비 시간이 길어. 정리까지 20분 안에 끝내고 싶어’처럼 이유를 주면 수정 방향이 분명해집니다.",
    b: "맞아요. ‘준비 시간이 길어’처럼 이유를 알려주면, 다음 답에 반영할 재료가 생겨요. 조건이 실제로 반영됐는지도 살펴보면 좋습니다."
  },
  truth: {
    a: "확신하는 말투가 정확한 시간을 보장하지는 않아요. 내 상황에서 준비와 정리까지 가능한지 살펴봐야 합니다.",
    b: "맞아요. 아이디어는 도움받고, 내 상황에 맞는지는 내가 판단해요. 딱 맞아 보이는 숫자도 근거가 없으면 예상으로 봅니다."
  }
};

document.querySelectorAll("[data-quiz]").forEach(quiz => {
  quiz.addEventListener("change", event => {
    quiz.querySelector(".feedback").textContent = feedback[quiz.dataset.quiz][event.target.value];
  });
});
document.querySelectorAll("[data-print]").forEach(button => {
  button.addEventListener("click", () => window.print());
});

document.querySelectorAll("[data-choice]").forEach(question => {
  question.addEventListener("change", event => {
    const message = event.target.dataset.feedback;
    if (message) question.querySelector(".feedback").textContent = message;
  });
});

document.querySelectorAll("[data-download]").forEach(button => {
  button.addEventListener("click", () => {
    const field = document.getElementById(button.dataset.download);
    if (!field) return;
    const content = 'value' in field ? field.value : field.textContent;
    const url = URL.createObjectURL(new Blob([content], { type: "text/plain;charset=utf-8" }));
    const link = document.createElement("a");
    link.href = url;
    link.download = button.dataset.filename || "AI-메모.txt";
    document.body.append(link);
    link.click();
    link.remove();
    window.setTimeout(() => URL.revokeObjectURL(url), 10000);
  });
});
