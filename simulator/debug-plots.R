library(tidyverse)

debug_log <- read_csv("debug.csv", col_names = c("time", "event", "class", "obj_id", "obj_name", "key", "value"), col_types = "dcccccc")

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
    filter(max_size > 100) %>%

    ggplot(aes(x = time,
               y = value/1500,
               color = obj_name)) +
    geom_step()

# Ongoing flows
flows <- debug_log %>%
    filter(class == "TCPFlow", str_detect(obj_name, "TCPFlow object", negate = TRUE)) %>%
    group_by(obj_id, obj_name) %>%
    summarize(arrival = min(time),
              end = max(time),
              n_size = sum(key == "size_packets"),
              size_packets = as.numeric(value[key == "size_packets"]),
              is_done = "Flow._done" %in% key)

flow_str <- "3\\[0->4\\]"
flow_str_not <- "43\\[0->4\\]"
#flow_str <- "1\\[9->0\\]"
debug_log %>%
    filter(str_detect(obj_name, flow_str) | str_detect(value, flow_str),
           str_detect(obj_name, flow_str_not, negate = TRUE),
           str_detect(value,    flow_str_not, negate = TRUE),
           !class %in% c("RotorNet", ""),
           str_detect(key, "log|rto|rtt|cwnd", negate = TRUE),
           event == "call"
           #time > .6, time < 1
           ) %>%
    mutate(packet_id = ifelse(class == "Packet", paste0("$", obj_id), str_extract(value, "\\$\\d+"))) %>%
    filter(!is.na(packet_id)) %>%

    ggplot(aes(x = time,
               y = reorder(paste(class, obj_name), as.integer(factor(obj_id, levels = unique(obj_id)))),
               #y = obj_name,
               label = key,
               group = packet_id,
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
               y = n*1500*8/5e-3/1e9,
               fill = is_good)) +
    geom_col(position = "stack") +
    coord_flip() +
    labs(x = NULL,
         y = "BW (Gb/s)")

