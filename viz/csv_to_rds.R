require(tidyverse)
require(data.table)

fns <- list.files(path = ".", pattern = "done-.*\\.csv")
for (fn in fns) {
    base_fn <- fn %>% str_split(pattern = "\\.csv") %>% .[[1]] %>% .[1]
    rds_fn  <- paste0(base_fn, ".rds")
    if (!file.exists(rds_fn)) {
        fn %>%
            fread(skip = "flow_id") %>%
            saveRDS(file = paste0(base_fn, ".rds"))
    }
    #file.remove(fn)
}
