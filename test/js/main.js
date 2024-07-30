let socket;
let client;
let client_1234;
let groups = {};
let reads = {};
let other_user_last_read = -1;
let other_user_last_read_idx = 0;
const user_id = '5588';
const other_user_id = '4321';
const rest_endpoint = 'http://maggie-kafka-1.thenetcircle.lab:9800';
const offline_endpoint = 'http://maggie-kafka-1.thenetcircle.lab:9810';
//const mqtt_endpoint = 'ws://maggie-kafka-1.thenetcircle.lab:1880/mqtt';
//const mqtt_endpoint = 'ws://maggie-kafka-3.thenetcircle.lab:1886/mqtt';
const mqtt_endpoint = 'ws://maggie-kafka-3.thenetcircle.lab:1887/mqtt';
//const mqtt_endpoint = 'wss://dino2.thenetcircle.com/mqtt';
//const mqtt_endpoint = 'mqtt://maggie-kafka-1.thenetcircle.lab:1883/mqtt';
const version = 'v1';


$(document).ready(initialize);

function initialize() {
    setup_mqtt();
    load_all_current_groups();

    $("a#next").click(load_more_messages);
    $("input#send-msg").click(send_message);
    $("input#send-mqtt").click(send_mqtt);
    $("input#send-mqtt-notification").click(send_notification);
    $("input#update-attachment").click(update_attachment);
    $("input#get-attachment").click(get_attachment);
    $("input#subscribe-irc").click(subscribe_irc);
    $("input#unsubscribe-irc").click(unsubscribe_irc);

    // since this element can be added dynamically, use on()
    $(document).on('click', 'a.join-group', open_conversation);
}

function setup_mqtt() {
    const settings = {
        clientId: `${user_id}_popp_1`,
        username: user_id,
        password: '5588',
        clean: true,
        rejectUnauthorized: false,
        protocolVersion: 5,
        qos: 1,
        keepalive: 600,
        properties: {
            sessionExpiryInterval: 600
        }
    }

    client = mqtt.connect(mqtt_endpoint, settings);
    client.on('connect', function () {
        console.log(`connected ${user_id}`)
        client.subscribe(`dms/testpopp/${user_id}`, {qos: 1}, function (err) {
            console.log(`subscribed to dms/testpopp/${user_id}`)
            if (err) {
                console.log(err);
            }
        });
        subscribe_irc();
    });

    client.on('message', on_mqtt_event);
    client.on("error", function(){
        console.log("error client_1234", arguments);
    });
}

function unsubscribe_irc() {
    client.unsubscribe(`dms/testpopp/irc`, {}, function (err) {
        console.log('unsubscribed from dms/testpopp/irc');
        if (err) {
            console.log(err);
        }
    });

    let data = {
        type: 'POST',
        contentType: 'application/json',
        dataType: 'json',
        processData: false,
        url: `${offline_endpoint}/api/v1/unsubscribe`,
        data: JSON.stringify({
            user_id: user_id,
            topic: 'dms/testpopp/irc'
        })
    }

    $.ajax(data);
}

function subscribe_irc() {
    client.subscribe(`dms/testpopp/irc`, {qos: 1}, function (err) {
        console.log(`subscribed to dms/testpopp/irc`)
        if (err) {
            console.log(err);
        }
    });
}

function send_mqtt() {
    const receiver = parseInt($("input#mqtt_receiver").val());
    const message_payload = $("textarea#mqtt_payload").val();

    // `dms/testpopp-${receiver}`,
    client.publish(
        `dms/testpopp/${receiver}`,
        message_payload,
        undefined,
        function (err) {
            console.log('error:', err)
        }
    )
}

function send_notification() {
    let receiver = parseInt($("input#mqtt_receiver_notification").val());
    let message_payload = $("textarea#mqtt_payload_notification").val();
    let group_id = $("input#mqtt_group_id_notification").val();

    call({
        url: `notification/send`,
        data: JSON.stringify({
            group_id: group_id,
            event_type: 'message',
            notification: [{
                user_ids: [receiver],
                data: {"payload": message_payload}
            }]
        }),
        success: function(resp) {
        }
    });
}

function on_mqtt_event(topic, message) {
    let json_data = ''

    try {
        json_data = JSON.parse(message.toString())
    }
    catch (e) {
        json_data = message.toString()
    }
    console.log('received event:', topic, json_data)

    const html_tag = topic.replaceAll('/', '-')

    // add a pretty-printed version to the event log
    $(`#events-${html_tag}`).prepend(JSON.stringify(json_data, null, 2) + "\n");

    switch (json_data["event_type"]) {
        case "message":
            handle_message_event(topic, json_data)
            break;

        case "read":
            handle_read_event(topic, json_data)
            break;

        default:
            break;
    }
}

function handle_message_event(topic, json_data) {
    if (!is_for_this_group(json_data)) {
        return;
    }
    // we don't care about other users' messages
    if (topic !== user_id) {
        return;
    }

    let history = $("pre#history");

    history.prepend(
        format_message(
            json_data.created_at,
            json_data.message_id,
            json_data.message_payload,
            json_data.sender_id
        )
    );

    if (is_new_group(json_data)) {
        get_group_name(json_data.group_id);
    }
}

function handle_read_event(topic, json_data) {
    let group_id = json_data["group_id"];
    let user_id = json_data["user_id"];
    let read_at = json_data["read_at"];

    if (!(group_id in reads)) {
        reads[group_id] = {};
    }
    if (!(user_id in reads[group_id])) {
        reads[group_id][user_id] = read_at;
    }
}

function call(values) {
    let data = {
        type: 'POST',
        contentType: 'application/json',
        dataType: 'json',
        processData: false
    }
    data = Object.assign({}, data, values);
    data["url"] = `${rest_endpoint}/${version}/${values["url"]}`;

    $.ajax(data);
}

function get_history(group_id, until, should_clear) {
    call({
        url: `groups/${group_id}/user/${user_id}/histories`,
        data: JSON.stringify({
            per_page: 10,
            until: until
        }),
        success: function(resp) {
            let history = $("pre#history");
            let until = $("input#until");

            if (should_clear) {
                history.html("");
            }
            console.log(resp);

            for (let i = 0; i < resp["last_reads"].length; i++) {
                if (resp["last_reads"][i]["user_id"] === parseInt(other_user_id)) {
                    other_user_last_read = resp["last_reads"][i]["last_read"];
                    break;
                }
            }

            for (let i = 0; i < resp["messages"].length; i++) {
                if (parseFloat(resp["messages"][i]["created_at"]) > other_user_last_read) {
                    other_user_last_read_idx = i;
                }
                else {
                    break;
                }
            }

            console.log(`last_read: ${other_user_last_read}`);
            console.log(`last_read_idx: ${other_user_last_read_idx}`);

            for (let i = 0; i < resp["messages"].length; i++) {
                let msg = resp["messages"][i];

                let payload = msg.message_payload;
                if (i === other_user_last_read_idx) {
                    payload = payload + ` (last read: ${other_user_id})`
                }

                history.append(
                    format_message(
                        msg.created_at,
                        msg.message_id,
                        payload,
                        msg.user_id
                    )
                );
                until.val(msg.created_at);
            }
        }
    });
}

function send_message() {
    let sender = $("input#sender").val();
    let receiver = parseInt($("input#receiver").val());
    let message_type = parseInt($("input#message_type").val());
    let message_payload = $("textarea#message_payload").val();

    call({
        url: `users/${sender}/send`,
        data: JSON.stringify({
            receiver_id: receiver,
            message_type: message_type,
            message_payload: message_payload
        }),
        success: function(resp) {
            $("input#message_id").val(resp["message_id"]);
            $("input#created_at").val(resp["created_at"]);
            $("input#group_id").val(resp["group_id"]);
            reset_file_id();
        }
    });
}

function get_group_name(group_id) {
    call({
        type: 'GET',
        url: `groups/${group_id}`,
        success: function(resp) {
            groups[group_id] = resp.name;
            add_group_link(group_id, resp.name);
        }
    });
}

function load_all_current_groups() {
    call({
        url: `users/${user_id}/groups`,
        data: JSON.stringify({
            per_page: 100,
            only_unread: false
        }),
        success: function(resp) {
            for (let i = 0; i < resp.length; i++) {
                let group_id = resp[i]["group"].group_id;
                let group_name = resp[i]["group"].name;

                groups[group_id] = resp.name;
                add_group_link(group_id, group_name);
            }
        }
    });
}

function update_attachment() {
    let sender = $("input#sender").val();
    let message_id = $("input#message_id").val();
    let file_id = $("input#file_id").val();
    let receiver = parseInt($("input#receiver").val());
    let created_at = parseFloat($("input#created_at").val());
    let message_type = parseInt($("input#message_type").val());

    call({
        url: `users/${sender}/message/${message_id}/attachment`,
        data: JSON.stringify({
            receiver_id: receiver,
            file_id: file_id,
            created_at: created_at,
            message_type: message_type,
            message_payload: "{\"width\":\"400\",\"height\":\"240\"}",
        }),
        success: function(resp) {
            $("input#message_id").val(resp["message_id"]);
            $("input#created_at").val(resp["created_at"]);
        }
    })
}

function get_attachment() {
    let file_id = $("input#get_file_id").val();
    let group_id = $("input#group_id").val();

    call({
        url: `groups/${group_id}/attachment`,
        data: JSON.stringify({
            group_id: group_id,
            file_id: file_id,
        }),
        success: function(data) {
            $("pre#attachment").html(JSON.stringify(data, null, 2));
        }
    })
}

function load_more_messages() {
    let group_id = $("input#current_group").val();
    let until = parseFloat($("input#until").val());

    get_history(group_id, until, false);
}

function open_conversation() {
    let group_id = $(this).attr('id');
    let now = new Date();
    let until = Math.round(now.getTime() / 1000);

    $("input#current_group").val(group_id);
    get_history(group_id, until, true);
}

function is_for_this_group(json_data) {
    return $("input#current_group").val() === json_data.group_id;
}

function is_new_group(json_data) {
    return !(json_data.group_id in groups);
}

function format_date(d) {
    return d.getFullYear() + "-" +
        ("0" + (d.getMonth() + 1)).slice(-2) + "-" +
        ("0" + d.getDate()).slice(-2) + " " +
        ("0" + d.getHours()).slice(-2) + ":" +
        ("0" + d.getMinutes()).slice(-2) + ":" +
        ("0" + d.getSeconds()).slice(-2);
}

function format_message(created_at_float, message_id, message_payload, sender_id) {
    let created_at = format_date(new Date(created_at_float * 1000));
    return `${created_at} - ${sender_id} - ${message_payload}\n`
}

function add_group_link(group_id, group_name) {
    $("div#groups").append(
        `<a href='#' class='join-group' id='${group_id}'>${group_name}</a><br />`
    );
}

function reset_file_id() {
    let file_id = parseInt((Math.random() * 1000000).toString());
    $("input#file_id").val(file_id);
    $("input#get_file_id").val(file_id);
}
