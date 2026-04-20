import { BridgeORM, Model, Raw } from 'bridgeorm-ts';

class Project extends Model {
    static table = 'projects';
    id: string;
    name: string;
    config: any;
}

async function demo() {
    await BridgeORM.connect('postgresql://localhost:5432/bridgeorm');
    
    const session = await BridgeORM.beginSession();
    
    const project = new Project();
    project.name = "BridgeORM TS Core";
    // Complex JSON config
    project.config = {
        engine: "Rust",
        features: ["FFI", "Tokio", "LazyStreams"],
        status: "Beta"
    };

    await session.save(project);
    console.log(`TS: Project stored with config: ${JSON.stringify(project.config)}`);
}

demo().catch(console.error);
