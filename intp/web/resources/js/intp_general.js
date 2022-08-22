function openTab(element, tabName) {
    var i;
    var x = document.getElementsByClassName("main");
    for (i = 0; i < x.length; i++) {
        x[i].style.display = "none";
    }
    document.getElementById(tabName).style.display = "block";
    document.getElementsByClassName("active")[0].className = "";
    element.parentNode.className="active";
}

function workbench() {
    var host = getElement("dbHost").value;
    var instance = getElement("dbInstance").value;
    var url = "http://" + host + ":80" + instance + "/in-time/core/workbench/";
    redirectOnNewTab(url);
}

function getElement(id) {
    var forms = document.getElementsByTagName("form");
    var val;
    if (null == forms || forms.length == 0) {
        val = document.getElementById(id);
    } else {
        var len = forms.length;
        for (var i = 0; i < len; i++) {
            var formId = forms[i].id;
            var elem = document.getElementById(formId + ":" + id);
            if (null == elem)
                continue;
            val = elem;
            break;
        }
    }
    if (null == val) {
        val = document.getElementById(id);
    }
    return val;
}

function redirectOnNewTab(url) {
    window.open(url, "_blank");
}

function updatePort(dbInstanceElement) {
    var portElem = getElement("dbPort");
    var instance = dbInstanceElement.value;
    var oldPort = portElem.value;
    if (null == oldPort || oldPort.length < 5) {
        portElem.value = "3" + instance + "15";
    } else {
        portElem.value = oldPort.charAt(0) + instance + oldPort.substring(3);
    }
}

function updateInstanceNumber(dbPortElement) {
    var port = dbPortElement.value;
    if (port.length !== 5) {
        return;
    }
    var newInstanceNumber = port.substring(1, 3);
    var dbInstanceElement = getElement("dbInstance");
    dbInstanceElement.value = newInstanceNumber;
}

function validateInstanceNumber(dbPortElement) {
    var port = dbPortElement.value;
    if (port.length !== 5) {
        return;
    }
    var newInstanceNumber = port.substring(1, 3);
    var oldInstanceNumber = window.location.href.split("/")[2].split(":")[1].substring(2);
    if (newInstanceNumber === oldInstanceNumber) {
        return;
    }
    var char0 = port.charAt(0);
    var rest = port.substring(3);
    dbPortElement.value = char0 + oldInstanceNumber + rest;
}


function setHostAndPort() {
    var url = window.location.href;
    var arr = url.split("/");
    var domport = arr[2].split(":");
    var domain = domport[0];
    var instanceNumber = domport[1].substring(2);

    var host = getElement("dbHost");
    var instance = getElement("dbInstance");
    var port = getElement("dbPort");
    if (domain === "localhost") {
        host.disabled = false;
        port.disabled = false;
    }
    host.value = domain;
    instance.value = instanceNumber;
    port.value = 3 + instanceNumber + 15;
}
