// 删除测量列前的确认；表单其余部分由服务端渲染，版本号来自用户所见页面。
document.addEventListener("submit", (event) => {
  const form = event.target;
  if (!(form instanceof HTMLFormElement)) return;
  if (form.dataset.confirm) {
    if (!window.confirm(form.dataset.confirm)) {
      event.preventDefault();
    }
  }
});

// 缺测核对页：datetime-local 控件没有“按 UTC 解释”的语义，这里预填当前
// UTC 的整点窗口（开始=当前整点，结束=6 小时后的整点），值本身即 UTC。
// 用户可自由修改；服务端始终按 UTC 解析并要求精确到分钟。
(function fillGapDefaults() {
  const start = document.getElementById("start-at");
  const end = document.getElementById("end-at");
  if (!start || !end) return;
  if (start.value || end.value) return;

  const pad = (n) => String(n).padStart(2, "0");
  const toLocalInput = (d) =>
    `${d.getUTCFullYear()}-${pad(d.getUTCMonth() + 1)}-${pad(d.getUTCDate())}` +
    `T${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())}`;

  const now = new Date();
  const startAt = new Date(
    Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate(),
             now.getUTCHours(), 0, 0, 0)
  );
  const endAt = new Date(startAt.getTime() + 6 * 60 * 60 * 1000);
  start.value = toLocalInput(startAt);
  end.value = toLocalInput(endAt);
})();

// 批次表格在窄屏下允许横向滚动，不额外处理。
