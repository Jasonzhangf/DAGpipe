use pipeline_runtime::{graph_topology, parse_graph_json, Graph};
use std::{env, fs, io::Write, path::PathBuf};

const PACKAGED_SKILL: &str = include_str!("../../.agents/skills/dagpipe-runtime/SKILL.md");

fn main() {
    if let Err(error) = run(env::args().skip(1).collect()) {
        eprintln!("dagpipe: {error}");
        std::process::exit(2);
    }
}

fn run(args: Vec<String>) -> Result<(), String> {
    let args: Vec<_> = args.iter().map(String::as_str).collect();
    match args.as_slice() {
        [command] if *command == "--version" || *command == "-V" => {
            println!("dagpipe {}", env!("CARGO_PKG_VERSION"));
            Ok(())
        }
        [command] if *command == "--help" || *command == "-h" => {
            print_help();
            Ok(())
        }
        ["modules", "list"] => {
            println!("graph.inspect\tinspect DAG topology and operator bindings");
            println!("graph.validate\tvalidate external DAG with one input and output (SESE)");
            println!("skill.install\tinstall the packaged project-usage skill");
            Ok(())
        }
        ["graph", "validate", path] => {
            let graph = load_graph(path)?;
            let topology = graph_topology(&graph).map_err(|error| error.to_string())?;
            println!(
                "valid DAG: {}@{} ({} nodes, {} edges, {} waves)",
                graph.id,
                graph.version,
                graph.nodes.len(),
                graph.edges.len(),
                topology.waves.len()
            );
            println!("operator bindings are syntactically present; project compile() remains the authoritative registry/schema/effect gate");
            Ok(())
        }
        ["graph", "inspect", path] => {
            let graph = load_graph(path)?;
            let topology = graph_topology(&graph).map_err(|error| error.to_string())?;
            println!("Graph {}@{}", graph.id, graph.version);
            println!("Operator bindings:");
            for node in &graph.nodes {
                println!(
                    "  {} -> {}@{}",
                    node.id, node.operator, node.operator_version
                );
            }
            println!("DAG execution waves:");
            for (index, wave) in topology.waves.iter().enumerate() {
                println!("  {}: {}", index + 1, wave.join(", "));
            }
            println!("Edges:");
            for edge in &graph.edges {
                println!("  {} --{}--> {}", edge.from, edge.arc_id, edge.to);
            }
            println!("Note: inspect checks DAG shape only; compile() resolves registered Operators and contracts.");
            Ok(())
        }
        ["skill", "install"] => install_skill(),
        ["sdk", "path"] => print_sdk_path(),
        _ => {
            print_help();
            Err("invalid command".into())
        }
    }
}

fn load_graph(path: &str) -> Result<Graph, String> {
    let contents =
        fs::read_to_string(path).map_err(|error| format!("cannot read graph `{path}`: {error}"))?;
    parse_graph_json(&contents).map_err(|error| error.to_string())
}

fn install_skill() -> Result<(), String> {
    let home = env::var_os("HOME")
        .map(PathBuf::from)
        .or_else(|| env::var_os("USERPROFILE").map(PathBuf::from))
        .ok_or_else(|| "cannot determine the user home directory".to_owned())?;
    let skill_path = home
        .join(".agents")
        .join("skills")
        .join("dagpipe-runtime")
        .join("SKILL.md");
    if fs::symlink_metadata(&skill_path).is_ok_and(|metadata| metadata.file_type().is_symlink()) {
        return Err(format!(
            "skill path is a symlink at {}; refusing to follow it",
            skill_path.display()
        ));
    }
    if skill_path.exists() {
        let existing = fs::read_to_string(&skill_path).map_err(|error| {
            format!(
                "cannot read existing skill `{}`: {error}",
                skill_path.display()
            )
        })?;
        if existing == PACKAGED_SKILL {
            println!(
                "DAGpipe skill already installed at {}",
                skill_path.display()
            );
            return Ok(());
        }
        if existing.starts_with("---\nname: dagpipe-runtime\n") {
            fs::write(&skill_path, PACKAGED_SKILL).map_err(|error| {
                format!(
                    "cannot update DAGpipe skill `{}`: {error}",
                    skill_path.display()
                )
            })?;
            println!("Updated DAGpipe skill at {}", skill_path.display());
            return Ok(());
        }
        return Err(format!(
            "skill already exists with different contents at {}; refusing to overwrite it",
            skill_path.display()
        ));
    }
    let parent = skill_path
        .parent()
        .ok_or_else(|| "invalid skill installation path".to_owned())?;
    fs::create_dir_all(parent).map_err(|error| {
        format!(
            "cannot create skill directory `{}`: {error}",
            parent.display()
        )
    })?;
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&skill_path)
        .map_err(|error| format!("cannot create skill `{}`: {error}", skill_path.display()))?;
    if let Err(error) = file.write_all(PACKAGED_SKILL.as_bytes()) {
        drop(file);
        let _ = fs::remove_file(&skill_path);
        return Err(format!(
            "cannot write skill `{}`: {error}",
            skill_path.display()
        ));
    }
    println!("Installed DAGpipe skill at {}", skill_path.display());
    Ok(())
}

fn print_sdk_path() -> Result<(), String> {
    let home = env::var_os("HOME")
        .map(PathBuf::from)
        .or_else(|| env::var_os("USERPROFILE").map(PathBuf::from))
        .ok_or_else(|| "cannot determine the user home directory".to_owned())?;
    println!("{}", home.join(".local/share/dagpipe/sdk").display());
    Ok(())
}

fn print_help() {
    println!(
        "DAGpipe framework governance CLI\n\n\
Usage:\n\
  dagpipe modules list\n\
  dagpipe graph validate <graph.json>\n\
  dagpipe graph inspect <graph.json>\n\
  dagpipe sdk path\n\
  dagpipe skill install\n\
  dagpipe --version\n\n\
The CLI validates one external object flow per SESE Graph and inspects\n\
operator bindings. Validate each project source as a separate Graph.\n\
Project module internals need not themselves be DAGs.\n\
Projects compile and execute their registered Operators through the Rust SDK."
    );
}
