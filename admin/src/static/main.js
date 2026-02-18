(() => {
  const $ = (s, root = document) => root.querySelector(s);

  // ===== Drawer =====
  const drawer = $("#drawer");
  const overlay = $("#overlay");

  const resetConfirm = () => {
    const confirmBox = $("#confirmBox");
    const toggleForm = $("#toggleForm");
    const deleteForm = $("#deleteForm");

    if (confirmBox) confirmBox.hidden = true;

    if (toggleForm) {
      toggleForm.hidden = true;
      toggleForm.action = "";
    }

    if (deleteForm) {
      deleteForm.hidden = true;
      deleteForm.action = "";
    }

    const title = $("#confirmTitle");
    const text = $("#confirmText");
    if (title) title.textContent = "Действие";
    if (text) text.textContent = "";
  };

  const openDrawer = () => {
    drawer?.classList.add("drawer--open");
    if (overlay) overlay.hidden = false;
    document.body.classList.add("no-scroll");
  };

  const closeDrawer = () => {
    drawer?.classList.remove("drawer--open");
    if (overlay) overlay.hidden = true;
    document.body.classList.remove("no-scroll");
    resetConfirm();
  };

  $("#openDrawer")?.addEventListener("click", openDrawer);
  $("#closeDrawer")?.addEventListener("click", closeDrawer);
  overlay?.addEventListener("click", closeDrawer);

  document.addEventListener("keydown", (e) => {
    if (e.key === "Escape" && drawer?.classList.contains("drawer--open")) closeDrawer();
  });

  $("#cancelAction1")?.addEventListener("click", resetConfirm);
  $("#cancelAction2")?.addEventListener("click", resetConfirm);

  // ===== Confirm actions (Toggle/Delete) =====
  document.addEventListener("click", (e) => {
    const btn = e.target.closest("button[data-action]");
    if (!btn) return;

    const { action, id, ticker, status } = btn.dataset;

    openDrawer();

    const confirmBox = $("#confirmBox");
    const toggleForm = $("#toggleForm");
    const deleteForm = $("#deleteForm");
    const title = $("#confirmTitle");
    const text = $("#confirmText");

    if (!confirmBox || !toggleForm || !deleteForm || !title || !text) return;

    confirmBox.hidden = false;

    if (action === "toggle") {
      title.textContent = "Toggle тикера";
      text.textContent = `Ticker: ${ticker} (id: ${id}). Сейчас: ${(status || "").toUpperCase()}. Переключить?`;

      toggleForm.hidden = false;
      deleteForm.hidden = true;
      toggleForm.action = `/toggle/${id}`;
    } else if (action === "delete") {
      title.textContent = "Удаление тикера";
      text.textContent = `Ticker: ${ticker} (id: ${id}). Удалить из списка? История останется.`;

      deleteForm.hidden = false;
      toggleForm.hidden = true;
      deleteForm.action = `/delete/${id}`;
    }
  });
})();
