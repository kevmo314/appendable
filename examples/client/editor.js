let activeEditor = "json";

var editor = ace.edit("editor");
editor.setTheme("ace/theme/chrome");

var jsonSession = ace.createEditSession(
  JSON.stringify(
    {
      where: [
        {
          operation: ">=",
          key: "trip_distance",
          value: 10,
        },
      ],
      orderBy: [
        {
          key: "trip_distance",
          direction: "ASC",
        },
      ],
      select: [
        "trip_distance",
        "VendorID",
        "passenger_count",
        "fare_amount",
        "tip_amount",
        "mta_tax",
      ],
    },
    null,
    2,
  ),
  "ace/mode/json",
);

var jsCode =
  "db\n" +
  "  .where('trip_distance', '>=', 10)\n" +
  "  .orderBy('trip_distance', 'ASC')\n" +
  "  .select([\n" +
  "      'trip_distance',\n" +
  "      'VendorID',\n" +
  "      'passenger_count',\n" +
  "      'fare_amount',\n" +
  "      'tip_amount',\n" +
  "      'mta_tax'\n" +
  "  ])\n" +
  "  .get();";

var jsSession = ace.createEditSession(jsCode, "ace/mode/javascript");

editor.setSession(jsonSession);

var jsonTab = document.getElementById("jsonTab");
var jsTab = document.getElementById("jsTab");

jsonTab.addEventListener("click", function () {
  editor.setSession(jsonSession);
  attachJsonEditorUX();
  activeEditor = "json";
  window.activeEditor = activeEditor;
});

jsTab.addEventListener("click", function () {
  editor.setSession(jsSession);
  activeEditor = "javascript";
  window.activeEditor = activeEditor;
});

function attachJsonEditorUX() {
  // NOTE: when composite indexes get supported, remove this UX feature
  // <---- start of UX feature ---->
  let isProgramChange = false;
  let lastEdited = "none";
  let prevWhereKey = "trip_distance";
  let prevOrderByKey = "trip_distance";

  function updateKey(editorContent) {
    try {
      let query = JSON.parse(editorContent);
      if (query.where && query.orderBy) {
        const whereKey = query.where[0].key;
        const orderByKey = query.orderBy[0].key;

        if (lastEdited === "where") {
          query.orderBy[0].key = whereKey;
        } else if (lastEdited === "orderBy") {
          query.where[0].key = orderByKey;
        }

        prevWhereKey = whereKey;
        prevOrderByKey = orderByKey;

        return JSON.stringify(query, null, 2);
      }
    } catch (e) {
      console.log("Error parsing JSON:", e.message);
      console.log("Incomplete string content:", editorContent);
    }
    return editorContent;
  }

  editor.getSession().on("change", function (e) {
    if (isProgramChange) {
      isProgramChange = false;
      return;
    }

    const cursorPosition = editor.getCursorPosition();
    const editorContent = editor.getSession().getValue();

    let query;
    try {
      query = JSON.parse(editorContent);
    } catch (e) {
      return;
    }

    const currentWhereKey = query.where ? query.where[0].key : "";
    const currentOrderByKey = query.orderBy ? query.orderBy[0].key : "";

    if (currentWhereKey !== prevWhereKey) {
      lastEdited = "where";
    } else if (currentOrderByKey !== prevOrderByKey) {
      lastEdited = "orderBy";
    }

    const updatedContent = updateKey(editorContent);

    if (updatedContent !== editorContent) {
      isProgramChange = true;

      const doc = editor.getSession().getDocument();
      doc.setValue(updatedContent);

      editor.moveCursorToPosition(cursorPosition);
      editor.clearSelection();
    }
  });

  // <---- end of UX feature ---->
}

attachJsonEditorUX();
window.activeEditor = activeEditor;
