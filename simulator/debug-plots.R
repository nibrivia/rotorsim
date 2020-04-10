library(tidyverse)
library(data.table)

setwd("~/rotorsim/simulator/")
debug_log <- fread("debug.csv",
                      col.names = c("time", "event", "class", "obj_id", "obj_name", "key", "value"),
                      #col_types = "dcccccc"
                   ) %>% 
    as_tibble()

# CWNDs
debug_log %>%
    filter(key == "cwnd") %>% 
    filter(as.numeric(value) > 1) %>% 
    #filter(str_detect(obj_name, "rotor")) %>%
    ggplot(aes(x = time,
               y = as.numeric(value),
               color = obj_name)) +
    geom_point() +
    geom_line() +
    #scale_y_log10() +
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
q_sizes <- debug_log %>%
    filter(event == "set",
           class == "NIC",
           str_detect(key, "q_size_B")) %>%
    group_by(obj_id) %>%
    mutate(value = as.numeric(value),
           max_size = max(value)) %>%
    ungroup()

q_sizes %>%
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
    summarize(arrival = min(time[key == "TCPFlow.start"]),
              #n_size = sum(key == "size_packets"),
              size_packets = as.numeric(value[key == "size_packets"]),
              #size_bits    = as.numeric(value[key == "size_bits"]),
              is_done = "Flow._done" %in% key,
              end = ifelse(is_done, as.numeric(time[key == "Flow._done"]), max(time))
    ) %>%
    separate(obj_name, c("tag", "id"), sep = " +") %>% 
    arrange(id)

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
    mutate(bw = (size_packets*1500*8)/1e9/((end-arrival)/1000)) %>% 
    arrange(bw) %>% 
    ggplot(aes(x = size_packets,
               y = bw,
               color = tag)) +
    geom_point() +
    scale_x_log10() +
    hrbrthemes::theme_ipsum_rc() +
    labs(x = "Flow size (packets)",
         y = "Effective rate-ish (Gb/s)",
         title = "Effective rate of done flows")

# Specific flow
flow_str <- "[0-9]+\\[[0-9]+->[0-9]+\\]"
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
    mutate(flow_str = flow) %>%
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
               count = (seq_along(time)-1)/n()) %>%
    group_by(flow_id, obj_name) %>%
        mutate(position = mean(count)) %>%
        ungroup()

# waterfall
one_flow <- flow_details %>%
    filter(flow_id == 0) %>%
    group_by(seq_num) %>%
        mutate(has_retransmit = any(retransmit_n > 1)) %>%
        ungroup()

one_flow %>%
    filter(class != "TCPFlow", class != "Packet") %>% 
    filter(class != "NIC") %>%
    filter(time < 26.5) %>% 
    #group_by(seq_num) %>% 
        #mutate(keep = )
    #filter(seq_num < 1630) %>% 
    filter(has_retransmit) %>%
    filter(seq_num %% 10 == 0) %>%
    #mutate(time = dense_rank(time)) %>%
    ggplot(aes(x = time,
               y = reorder(paste(class, obj_name), position),
               #y = obj_name,
               label = key,
               group = paste(packet_id, seq_num, class == "TCPFlow"),
               color = as_factor(seq_num)
               #color = as_factor(retransmit_n)
               #color = (seq_num > 888) + (seq_num > 25000/2)
           )) +
    geom_point() +
    geom_line() +
    labs(y = NULL, x = NULL) +
    #guides(color = F) +
    hrbrthemes::theme_ipsum_rc()

# waterfall for 1 ToR
tor_details <- flow_details %>%
    #filter(time < .2) %>% 
    #filter(str_detect(obj_name, "Tor 28")) %>%
    group_by(flow_id, seq_num, packet_id) %>%
        filter(any(str_detect(obj_name, "Tor 28"))) %>%
        ungroup() %>% 
    group_by(flow_id, seq_num) %>% 
        mutate(has_retransmit= any(retransmit_n > 1))

tor_details %>%
    filter(time < 9) %>% 
    filter(has_retransmit) %>% 
    filter(class != "Packet",
           class != "TCPFlow") %>% 
    #mutate(time = dense_rank(time)) %>%
    ggplot(aes(x = time,
               y = reorder(paste(class, obj_name), position),
               group = paste(flow_str, seq_num, packet_id),
               color = as_factor(seq_num))) +
    geom_line() +
    geom_point() +
    labs(x=NULL, y = NULL) +
    #guides(color = F) +
    hrbrthemes::theme_ipsum_rc()

# Link utilization
#queue_utilization <- debug_log %>%
queue_utilization <- read_csv("ni") %>%
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

