document.addEventListener("DOMContentLoaded", function () {
    var script = document.createElement("script");
    script.type = "module";
    script.id = "runllm-widget-script"

    script.src = "https://widget.runllm.com";

    script.setAttribute("runllm-keyboard-shortcut", "Mod+j"); // cmd-j or ctrl-j to open the widget.
    script.setAttribute("runllm-name", "Delta Lake");
    script.setAttribute("runllm-position", "BOTTOM_RIGHT");
    script.setAttribute("runllm-assistant-id", "268");
    script.setAttribute("runllm-theme-color", "#008ED9");
    script.setAttribute("runllm-brand-logo", "https://delta.io/static/delta-lake-logo-a1c0d80d23c17de5f5d7224cb40f15dc.svg");
    script.setAttribute("runllm-community-type", "slack");
    script.setAttribute("runllm-community-url", "https://go.delta.io/slack");
    script.setAttribute("runllm-disable-ask-a-person", "true");

    script.async = true;
    document.head.appendChild(script);
});