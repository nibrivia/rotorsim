library(tidyverse)

gen_cdf_csv <- function(flows_fn) {
    flows <- read_csv(flows_fn)
    if (!"start" %in% colnames(flows)) {
        arrivals <- flows %>% rename(end = time_ms)
        flows <- read_csv(gsub(".csv", "-flows.csv", flows_fn)) %>%
            rename(start = arrival, flow_id = id, size = size_bytes) %>%
            left_join(arrivals, by = "flow_id")
    }
    cdf <- flows %>%
        mutate(fct = end-start,
               cdf_all = percent_rank(fct)) %>%
        group_by(size) %>%
            mutate(cdf_size = percent_rank(fct)) %>%
            sample_n(min(1000, n())) %>%
        arrange(cdf_all) %>%
        select(cdf_all, cdf_size, size, fct)

    return(cdf)
}


csvs <- list.files(pattern = ".csv")
for (fn in csvs) {
    cdf <- gen_cdf_csv("../data/257-33:0-0.1-1000ms.csv")
    write_csv(cdf, path = paste0("cdfs/cdf-", fn))
}
