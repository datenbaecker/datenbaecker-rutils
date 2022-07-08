
#' Connection to dbt Project
#'
#' @param project_path Path to dbt project
#' @param dbt_path Path to dbt executable (default is {dbt_project}/venv/bin/dbt)
#'
#' @return a dbt connection
#' @export
#'
dbt_conn <- function(project_path, dbt_path = NULL) {
    if (is.null(dbt_path)) {
        dbt_path <- paste0(project_path, "venv/bin/dbt")
    }
    structure(list(dbt_path = dbt_path, project_path = project_path), class = "dbt")
}

#' Get dbt Project Name
#'
#' @param dbt_conn dbt connection
#'
#' @return The project name (character)
#' @export
#'
#' @import dplyr
#' @importFrom yaml yaml.load_file
#'
get_project_name <- function(dbt_conn) {
    dbt_project <- dbt_conn$project_path %>%
        paste0("/dbt_project.yml") %>%
        yaml::yaml.load_file()
    dbt_project$name
}

create_var_dict <- function(...) {
    vars <- list(...)
    paste0(
        "{",
        paste0('"', names(vars), '": "', vars, '"', collapse = ","),
        "}"
    )

}

#' Compile the SQL of a dbt Analysis
#'
#' @param analysis_name Name of the analysis (without .sql file ending)
#' @param dbt_conn dbt connection
#' @param ... Variables to pass to dbt compile
#'
#' @return The compiled SQL statement
#' @export
#' @importFrom readr read_file
#'
compile_analyses <- function(analysis_name, dbt_conn, ...) {
    compile_cmd <- paste0(dbt_conn$dbt_path, " compile --vars '", create_var_dict(...), "'")
    cmd <- paste0("cd ", dbt_conn$project_path, ";", compile_cmd)
    system(cmd)
    sql_path <- paste0(
        dbt$project_path, "/target/compiled/",
        get_project_name(dbt_conn), "/analyses/",
        analysis_name, ".sql"
    )
    readr::read_file(sql_path)
}
