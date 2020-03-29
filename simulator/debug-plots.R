library(tidyverse)

setwd("~/rotorsim/simulator/")
debug_log <- read_csv("debug.csv",
                      col_names = c("time", "event", "class", "obj_id", "obj_name", "key", "value"),
                      col_types = "dcccccc")

# CWNDs
debug_log %>%
    filter(key == "cwnd",
           #time < .4,
           str_detect(obj_name, "rotor")) %>%
    ggplot(aes(x = time,
               y = as.numeric(value),
               color = obj_name)) +
    geom_point() +
    geom_line() +
    scale_y_log10() +
    guides(color = F)

packet_str <- "1\\[8->20\\]#0"
debug_log %>%
    filter(str_detect(obj_name, packet_str) | str_detect(value, packet_str) | str_detect(obj_name, "1\\[8->20\\]$"),
           !str_detect(key, "log|rtt|rto|cwnd|timeout")) %>%
    mutate(packet_id = ifelse(class == "Packet",
                              paste0("$", obj_id),
                              str_extract(value, "\\$\\d+"))) %>%
    ggplot(aes(x = time, y = packet_id, color = packet_id, group = packet_id, label = key)) +
    geom_point() +
    geom_line() +
    geom_text(color = "black",
              angle = 25) +
    facet_wrap(~event)

class_levels <- c("Packet", "TCPFlow", "Server", "ToRSwitch", "NIC", "RotorSwitch", "Switch")


debug_log %>%
    filter(str_detect(obj_name, packet_str) | str_detect(value, packet_str) | str_detect(obj_name, "1\\[8->20\\]$"),
           !str_detect(key, "log|rtt|rto|cwnd"),
           time < .025, time > .01) %>%
    arrange(time) %>%
    mutate(packet_id = ifelse(class == "Packet", paste0("$", obj_id), str_extract(value, "\\$\\d+"))) %>%

    ggplot(aes(x = time,
               y = reorder(paste(class, obj_name), as.integer(ordered(class, levels = class_levels))),
               color = packet_id,
               group = packet_id,
               label = key)) +
    geom_point() +
    geom_line() +
    geom_text(color = "black", angle = 25) +
    facet_wrap(~event)


# Queue sizes
debug_log %>%
    filter(event == "set",
           class == "NIC",
           str_detect(key, "q_size_B")) %>%
    group_by(obj_id) %>%
        mutate(value = as.numeric(value),
               max_size = max(value)) %>%
        ungroup() %>%
    mutate(queue_rank = dense_rank(-max_size)) %>%
    filter(queue_rank <= 10) %>%


    ggplot(aes(x = time,
               y = value/1500,
               color = reorder(obj_name, queue_rank))) +
    geom_step() +
    hrbrthemes::theme_ipsum_rc() +
    labs(x = NULL, y = "Queue size (packets)",
         title = "Top 10 largest queues (by max size)",
         color = "Largest queues")

# Ongoing flows
flows <- debug_log %>%
    filter(class == "TCPFlow", str_detect(obj_name, "TCPFlow object", negate = TRUE)) %>%
    group_by(obj_id, obj_name) %>%
        summarize(arrival = min(time),
                  n_size = sum(key == "size_packets"),
                  size_packets = as.numeric(value[key == "size_packets"]),
                  #size_bits    = as.numeric(value[key == "size_bits"]),
                  is_done = "Flow._done" %in% key,
                  end = ifelse(is_done, as.numeric(time[key == "Flow._done"]), max(time))
                  ) %>%
    separate(obj_name, c("tag", "id"), sep = " +")

# General flows
flows %>%
    ggplot(aes(x = arrival,
               y = end-arrival,
               color = is_done)) +
    geom_point() +
    geom_segment(aes(xend = arrival, yend = 0),
                 size = .2) +
    hrbrthemes::theme_ipsum_rc() +
    labs(x = NULL, y = "FCT")

flows %>%
    filter(is_done) %>%
    ggplot(aes(x = arrival,
               y = (size_packets*1500*8)/1e9/((end-arrival)/1000),
               color = tag)) +
    geom_point() +
    hrbrthemes::theme_ipsum_rc() +
    labs(x = "Flow start (ms)",
         y = "Effective rate-ish (Gb/s)",
         title = "Effective rate of done flows")

# Specific flow
flow_str <- "[0-9]+\\[[0-9]+->[0-9]+\\]"
#flow_str <- "1\\[9->0\\]"
flow_details <-
    debug_log %>%
    filter(str_detect(obj_name, flow_str) | str_detect(value, flow_str),
           !class %in% c("RotorNet", ""),
           str_detect(key, "log|rto|rtt|cwnd", negate = TRUE),
           event == "call"
           #time > .6, time < 1
           ) %>%
    mutate(packet = ifelse(class == "Packet", obj_name, str_replace(value, ",.*$", "") )) %>%
    filter(str_detect(packet, paste0(flow_str, "#"))) %>%
    separate(packet, c("packet_seq", "packet_id"), sep = " \\$") %>%
    separate(packet_seq, c("flow", "seq_num"), sep = "#") %>%
    separate(flow, c("flow_id", "srcdst"), sep = "\\[") %>%
    mutate(srcdst = str_replace(srcdst, "\\]", "")) %>%
    separate(srcdst, c("src", "dst"), sep = "->") %>%
    mutate(flow_id = as.integer(flow_id),
           src     = as.integer(src),
           dst     = as.integer(dst),
           seq_num = as.integer(seq_num)
           ) %>%
    group_by(flow_id, seq_num, packet_id) %>%
        mutate(creation = min(time),
               #timeout_n = str(time[str_detect(key, "timeout")]),
               #timeout_t = time[str_detect(key, "timeout")],
               #srcrecv_n = str(time[str_detect(key, "src_recv")]),
               #srcrecv_t = time[str_detect(key, "src_recv")],
               #timedout = timeout_t < srcrect_t,
               ) %>%
    group_by(flow_id, seq_num) %>%
        arrange(time) %>%
        mutate(retransmit_n = dense_rank(creation),
               count = seq_along(time)-1) %>%
    group_by(flow_id, obj_name) %>%
        mutate(position = mean(count)) %>%
        ungroup()

flow_details %>%
    filter(flow_id == 73, seq_num < 3) %>%
    #filter(class != "NIC") %>%
    mutate(time = dense_rank(time)) %>%
    ggplot(aes(x = time,
               y = reorder(paste(class, obj_name), position),
               #y = obj_name,
               label = key,
               group = paste(packet_id, class == "TCPFlow"),
               color = packet_id)) +
    geom_point() +
    geom_line() +
    #geom_text(color = "black",
    #          angle = 30) +
    labs(y = NULL, x = NULL) +
    guides(color = F) +
    hrbrthemes::theme_ipsum_rc()

# Link utilization
queue_utilization <- debug_log %>%
    filter(class == "NIC",
           str_detect(key, "enq"), event == "call") %>%

    separate(value, c("pkt", "id"), " \\$") %>%
    group_by(obj_name, pkt) %>%
        mutate(n_transmit = dense_rank(id),
               is_good = n_transmit == 1) %>%

    group_by(obj_name, is_good) %>%
        summarize(n = n()) %>%
    group_by(obj_name) %>%
        mutate(n_obj = sum(n)) %>%
    separate(obj_name, c("src", "dst"), "->")

queue_utilization %>%
    ggplot(aes(x = reorder(paste(src, dst, sep = " -> "), n_obj),
               y = n*1500*8/10e-3/1e9,
               fill = is_good)) +
    geom_col(position = "stack") +
    coord_flip() +
    hrbrthemes::theme_ipsum_rc() +
    labs(x = NULL, y = "BW (Gb/s)",
         fill = "good?")

