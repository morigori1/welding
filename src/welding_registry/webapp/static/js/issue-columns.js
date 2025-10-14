document.addEventListener("DOMContentLoaded", () => {
  const selectedList = document.getElementById("selected-columns");
  const availableSelect = document.getElementById("available-columns");
  if (!selectedList || !availableSelect) {
    return;
  }

  let activeItem = selectedList.querySelector("li");
  if (activeItem) {
    activeItem.classList.add("selected");
  }

  function setActive(item) {
    if (!item) return;
    if (activeItem) {
      activeItem.classList.remove("selected");
    }
    activeItem = item;
    activeItem.classList.add("selected");
  }

  function createListItem(value, label) {
    const li = document.createElement("li");
    li.dataset.column = value;
    const span = document.createElement("span");
    span.textContent = label;
    const hidden = document.createElement("input");
    hidden.type = "hidden";
    hidden.name = "columns";
    hidden.value = value;
    li.appendChild(span);
    li.appendChild(hidden);
    return li;
  }

  function optionExists(select, value) {
    return Array.from(select.options).some((opt) => opt.value === value);
  }

  function insertOptionSorted(select, value, label) {
    const option = document.createElement("option");
    option.value = value;
    option.textContent = label;
    const opts = Array.from(select.options);
    const insertIndex = opts.findIndex((opt) => opt.text.localeCompare(label, "ja") > 0);
    if (insertIndex === -1) {
      select.appendChild(option);
    } else {
      select.insertBefore(option, select.options[insertIndex]);
    }
  }

  function addColumn(value, label) {
    if (!value) return;
    if (selectedList.querySelector(`li[data-column="${value}"]`)) {
      return;
    }
    const item = createListItem(value, label);
    selectedList.appendChild(item);
    setActive(item);
    Array.from(availableSelect.options).forEach((opt) => {
      if (opt.value === value) {
        opt.remove();
      }
    });
  }

  function removeActive() {
    if (!activeItem) return;
    const value = activeItem.dataset.column;
    const label = activeItem.querySelector("span")?.textContent || value;
    activeItem.remove();
    if (!optionExists(availableSelect, value)) {
      insertOptionSorted(availableSelect, value, label);
    }
    activeItem = selectedList.querySelector("li") || null;
    if (activeItem) {
      activeItem.classList.add("selected");
    }
  }

  function moveActive(offset) {
    if (!activeItem) return;
    const siblings = Array.from(selectedList.querySelectorAll("li"));
    const index = siblings.indexOf(activeItem);
    const targetIndex = index + offset;
    if (targetIndex < 0 || targetIndex >= siblings.length) {
      return;
    }
    const reference = offset < 0 ? siblings[targetIndex] : siblings[targetIndex].nextSibling;
    selectedList.insertBefore(activeItem, reference);
  }

  selectedList.addEventListener("click", (event) => {
    const item = event.target.closest("li");
    if (item) {
      setActive(item);
    }
  });

  selectedList.addEventListener("dblclick", () => {
    removeActive();
  });

  availableSelect.addEventListener("dblclick", () => {
    const option = availableSelect.selectedOptions[0];
    if (option) {
      addColumn(option.value, option.textContent || option.value);
    }
  });

  document.querySelectorAll(".column-buttons .btn").forEach((button) => {
    button.addEventListener("click", () => {
      const action = button.dataset.action;
      if (action === "add") {
        const option = availableSelect.selectedOptions[0];
        if (option) {
          addColumn(option.value, option.textContent || option.value);
        }
      } else if (action === "remove") {
        removeActive();
      } else if (action === "up") {
        moveActive(-1);
      } else if (action === "down") {
        moveActive(1);
      }
    });
  });
});
