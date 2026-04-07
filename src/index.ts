import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { SSEServerTransport } from "@modelcontextprotocol/sdk/server/sse.js";
import {
  ListToolsRequestSchema,
  CallToolRequestSchema,
} from "@modelcontextprotocol/sdk/types.js";
import axios from "axios";
import http from "node:http";

// ── Configuration ─────────────────────────────────────────────────────────────
const PIPEFY_TOKEN = process.env.PIPEFY_TOKEN ?? "";
const GRAPHQL_URL = "https://api.pipefy.com/graphql";

// ── Fuzzy matching ────────────────────────────────────────────────────────────
function normalizeName(s: string): string {
  return s.toLowerCase().trim().normalize("NFD").replace(/\p{Mn}/gu, "");
}

function similarity(a: string, b: string): number {
  const tokensA = new Set(normalizeName(a).split(/\s+/).filter(Boolean));
  const tokensB = new Set(normalizeName(b).split(/\s+/).filter(Boolean));
  const union = new Set([...tokensA, ...tokensB]);
  if (union.size === 0) return 0;
  let intersection = 0;
  for (const t of tokensA) if (tokensB.has(t)) intersection++;
  return intersection / union.size;
}

// ── Schema cache ──────────────────────────────────────────────────────────────
// eslint-disable-next-line @typescript-eslint/no-explicit-any
let schemaCache: any = null;
let schemaCacheTime = 0;
const SCHEMA_CACHE_TTL = 30 * 60 * 1000;

// ── Expert context ─────────────────────────────────────────────────────────────
const PIPEFY_EXPERT_CONTEXT =
  "Especialista em Pipefy. Referência técnica:\n" +
  "HIERARQUIA: Organization > Pipe > Phase > Field > Card. Databases são tabelas independentes.\n" +
  "TIPOS DE CAMPO: short_text, long_text, number, date, datetime, select, radio, checkbox, attachment, connection (liga pipe a pipe), table (liga a database), email, phone, currency, assignee, label, due_date\n" +
  "TRIGGERS: card.create, card.move, card.done, card.expired, card.late, card.field_update\n" +
  "CONDITIONS: eq, neq, gt, lt, gte, lte, contains, not_contains, is_empty, is_not_empty\n" +
  "ACTIONS: move_card, update_field, create_card, send_email, create_webhook_notification, set_due_date, assign_card\n" +
  "CONEXÕES: connection field (pipe→pipe), table field (pipe→database), create_card action (pipe→pipe via automação), create_webhook_notification (pipe→URL externa)\n" +
  "GRAPHQL PARA MAKE: POST https://api.pipefy.com/graphql, header Authorization: Bearer TOKEN, body: { query, variables }";

// ── GraphQL helper ────────────────────────────────────────────────────────────
// eslint-disable-next-line @typescript-eslint/no-explicit-any
async function graphql(query: string, variables?: object): Promise<any> {
  const resp = await axios.post(
    GRAPHQL_URL,
    { query, variables },
    {
      headers: {
        Authorization: `Bearer ${PIPEFY_TOKEN}`,
        "Content-Type": "application/json",
      },
    }
  );
  if (resp.data.errors && resp.data.errors.length > 0) {
    throw new Error(resp.data.errors[0].message as string);
  }
  return resp.data.data;
}

// ── Internal: get_me (cached per process lifetime) ───────────────────────────
type MeResult = { id: string; name: string; email: string; organizations: Array<{ id: string; name: string }> };
let meCache: MeResult | null = null;

async function getMe(): Promise<MeResult> {
  if (meCache) return meCache;
  const data = await graphql(`{ me { id name email organizations { id name } } }`);
  meCache = {
    id: data.me.id as string,
    name: data.me.name as string,
    email: data.me.email as string,
    organizations: data.me.organizations as Array<{ id: string; name: string }>,
  };
  return meCache;
}

/**
 * Resolve org_id a partir de um ID numérico, nome, ou fallback para a primeira org.
 * - ID numérico → usa direto.
 * - Nome → fuzzy match contra as organizações do token.
 *   - Alta confiança (score ≥ 0.7, sem empate) → assume automaticamente.
 *   - Ambíguo → lança erro listando candidatos.
 * - Vazio e 1 org → usa a única disponível.
 * - Vazio e várias → lança erro pedindo o org_id ou nome.
 */
async function resolveOrgId(org_id?: string): Promise<{ org_id: string; org_name: string }> {
  const me = await getMe();
  const orgs = me.organizations;

  // No input: if only one org, use it; otherwise ask
  if (!org_id) {
    if (orgs.length === 1) return { org_id: orgs[0].id, org_name: orgs[0].name };
    const names = orgs.map(o => `"${o.name}" (${o.id})`).join(", ");
    throw new Error(`Múltiplas organizações disponíveis: ${names}. Informe org_id ou o nome da organização.`);
  }

  // Numeric ID → use directly
  if (/^\d+$/.test(org_id)) return { org_id, org_name: "" };

  // Name-based resolution
  interface OrgMatch { id: string; name: string; score: number }
  const matches: OrgMatch[] = orgs
    .map(o => ({ id: o.id, name: o.name, score: similarity(org_id, o.name) }))
    .filter(m => m.score > 0.3)
    .sort((a, b) => b.score - a.score);

  if (matches.length === 0) {
    const names = orgs.map(o => `"${o.name}" (${o.id})`).join(", ");
    throw new Error(`Nenhuma organização encontrada com nome similar a "${org_id}". Disponíveis: ${names}.`);
  }

  const top = matches[0];
  const second = matches[1];
  if (top.score >= 0.7 && (!second || top.score > second.score + 0.15)) {
    return { org_id: top.id, org_name: top.name };
  }

  const candidates = matches.slice(0, 4).map(m => `"${m.name}" (${m.id})`).join(", ");
  throw new Error(`Nome ambíguo "${org_id}". Candidatos: ${candidates}. Use o org_id numérico diretamente.`);
}

// ── Types ─────────────────────────────────────────────────────────────────────
interface PipeField {
  id: string;
  label: string;
  type: string;
  description?: string;
  required?: boolean;
  options?: string[];
  connectedPipe?: { id: string; name: string } | null;
  connectedTable?: { id: string; name: string } | null;
  conditions?: Array<{
    id: string;
    action: string;
    condition_expressions: Array<{
      field: { id: string; label: string; type: string };
      operator: string;
      value: string;
    }>;
  }>;
}

interface Phase {
  id: string;
  name: string;
  description?: string;
  cards_count?: number;
  fields?: PipeField[];
}

interface PipeStructure {
  id: string;
  name: string;
  description?: string;
  status?: string;
  phases: Phase[];
  start_form_fields?: PipeField[];
  webhooks?: Array<{ id: string; name: string; url: string; actions: string[]; active: boolean }>;
  members?: Array<{ user: { id: string; name: string; email: string }; role: string }>;
}

interface AutomationAction {
  type: string;
  phase?: { id: string; name: string } | null;
  field?: { id: string; label: string } | null;
  value?: string | null;
  pipe?: { id: string; name: string } | null;
  table?: { id: string; name: string } | null;
  url?: string | null;
}

interface Automation {
  id: string;
  name: string;
  active: boolean;
  trigger: {
    type: string;
    conditions?: Array<{ id: string; field_id: string; operator: string; value: string }>;
    phase?: { id: string; name: string } | null;
    field?: { id: string; label: string } | null;
  };
  actions: AutomationAction[];
}

// ── Group 1: Investigation ─────────────────────────────────────────────────────

async function listPipes(org_id?: string) {
  const { org_id: resolvedOrgId, org_name } = await resolveOrgId(org_id);
  const data = await graphql(
    `query($orgId: ID!) {
      organization(id: $orgId) {
        pipes {
          id name status description
          phases { id name cards_count }
          members { user { id name email } role }
        }
      }
    }`,
    { orgId: resolvedOrgId }
  );
  const pipes = data.organization.pipes as Array<{
    id: string;
    name: string;
    status: string;
    description: string;
    phases: Array<{ id: string; name: string; cards_count: number }>;
    members: Array<{ user: { id: string; name: string; email: string }; role: string }>;
  }>;

  return {
    org_id: resolvedOrgId,
    org_name: org_name || resolvedOrgId,
    pipes: pipes.map((p) => ({
      id: p.id,
      name: p.name,
      status: p.status,
      phases_count: p.phases.length,
      total_cards: p.phases.reduce((acc, ph) => acc + (ph.cards_count ?? 0), 0),
      members_count: p.members.length,
    })),
  };
}

async function getPipeStructure(pipe_id: string) {
  const data = await graphql(
    `query($id: ID!) {
      pipe(id: $id) {
        id name description status
        phases {
          id name description cards_count
          fields {
            id label type description required options
            connectedPipe { id name }
            connectedTable { id name }
            conditions {
              id action
              condition_expressions {
                field { id label type }
                operator
                value
              }
            }
          }
        }
        start_form_fields {
          id label type required options
          connectedPipe { id name }
          connectedTable { id name }
        }
        webhooks { id name url actions active }
        members { user { id name email } role }
      }
    }`,
    { id: pipe_id }
  );

  const pipe = data.pipe as PipeStructure;

  const allFields = [
    ...(pipe.start_form_fields ?? []),
    ...pipe.phases.flatMap((ph) => ph.fields ?? []),
  ];

  const connection_fields = allFields
    .filter((f) => f.connectedPipe)
    .map((f) => ({ field_id: f.id, label: f.label, connected_pipe: f.connectedPipe }));

  const table_fields = allFields
    .filter((f) => f.connectedTable)
    .map((f) => ({ field_id: f.id, label: f.label, connected_table: f.connectedTable }));

  const external_triggers = (pipe.webhooks ?? []).filter((w) => w.active);

  return {
    ...pipe,
    connection_fields,
    table_fields,
    external_triggers,
  };
}

async function getPipeAutomations(pipe_id: string) {
  const data = await graphql(
    `query($id: ID!) {
      pipe(id: $id) {
        automations {
          id name active
          trigger {
            type
            conditions { id field_id operator value }
            phase { id name }
            field { id label }
          }
          actions {
            type
            phase { id name }
            field { id label }
            value
            pipe { id name }
            table { id name }
            url
          }
        }
      }
    }`,
    { id: pipe_id }
  );

  const automations = data.pipe.automations as Automation[];

  return automations.map((a) => {
    let classification = "interno";
    const hasWebhook = a.actions.some((act) => act.type === "create_webhook_notification");
    const hasExternalCard = a.actions.some(
      (act) => act.type === "create_card" && act.pipe && act.pipe.id !== pipe_id
    );
    const hasEmail = a.actions.some((act) => act.type === "send_email");

    if (hasWebhook) classification = "webhook";
    else if (hasExternalCard) classification = "cria_card_externo";
    else if (hasEmail) classification = "email";

    const make_webhook_url = a.actions
      .filter(
        (act) =>
          act.url &&
          (act.url.includes("make.com") || act.url.includes("hook"))
      )
      .map((act) => act.url);

    return {
      ...a,
      classification,
      make_webhook_url: make_webhook_url.length > 0 ? make_webhook_url : null,
    };
  });
}

async function listDatabases(org_id?: string) {
  const { org_id: resolvedOrgId, org_name } = await resolveOrgId(org_id);
  const data = await graphql(
    `query($orgId: ID!) {
      organization(id: $orgId) {
        tables {
          edges {
            node {
              id name description
              table_fields { id label type required options }
              members { user { id name } role }
            }
          }
        }
      }
    }`,
    { orgId: resolvedOrgId }
  );

  const tables = data.organization.tables.edges as Array<{
    node: {
      id: string;
      name: string;
      description: string;
      table_fields: PipeField[];
      members: Array<{ user: { id: string; name: string }; role: string }>;
    };
  }>;

  return {
    org_id: resolvedOrgId,
    org_name: org_name || resolvedOrgId,
    databases: tables.map((e) => ({
      id: e.node.id,
      name: e.node.name,
      description: e.node.description,
      fields: e.node.table_fields,
      members_count: e.node.members.length,
    })),
  };
}

async function getDatabaseStructure(table_id: string) {
  const data = await graphql(
    `query($id: ID!) {
      table(id: $id) {
        id name description
        table_fields { id label type required options }
        table_records(first: 3) {
          edges { node { id title record_fields { field { id label } value } } }
        }
      }
    }`,
    { id: table_id }
  );
  return data.table;
}

async function traceCardJourney(card_id: string) {
  const data = await graphql(
    `query($id: ID!) {
      card(id: $id) {
        id title
        pipe { id name }
        current_phase { id name }
        fields { field { id label type } value }
        comments(first: 50) {
          edges { node { id text created_at author { name } } }
        }
        child_relations {
          name
          cards { id title pipe { id name } current_phase { id name } }
        }
        parent_relations {
          name
          cards { id title pipe { id name } current_phase { id name } }
        }
      }
    }`,
    { id: card_id }
  );

  const card = data.card;
  return {
    card_id: card.id,
    title: card.title,
    pipe: card.pipe,
    current_phase: card.current_phase,
    fields: card.fields,
    connected_cards: {
      parents: card.parent_relations,
      children: card.child_relations,
    },
    comments: card.comments?.edges?.map(
      (e: { node: unknown }) => e.node
    ) ?? [],
  };
}

// ── Group 2: Connection Mapping ───────────────────────────────────────────────

interface ConnectionEntry {
  type: "pipe_to_pipe" | "pipe_to_database" | "automation_creates_card" | "webhook_to_make";
  source: { id: string; name: string; type: "pipe" | "database" };
  target: { id: string; name: string; type: "pipe" | "database" | "url" };
  via: string;
  field_id: string | null;
  automation_id: string | null;
}

async function mapFullConnections(org_id?: string) {
  const { org_id: resolvedOrgId } = await resolveOrgId(org_id);

  const [pipesResult, dbResult] = await Promise.all([
    listPipes(resolvedOrgId),
    listDatabases(resolvedOrgId),
  ]);

  const connections: ConnectionEntry[] = [];
  const make_webhooks: Array<{ automation_id: string; pipe_id: string; url: string }> = [];

  for (const pipe of pipesResult.pipes) {
    const [structure, automations] = await Promise.all([
      getPipeStructure(pipe.id),
      getPipeAutomations(pipe.id),
    ]);

    for (const cf of structure.connection_fields) {
      if (cf.connected_pipe) {
        connections.push({
          type: "pipe_to_pipe",
          source: { id: pipe.id, name: pipe.name, type: "pipe" },
          target: { id: cf.connected_pipe.id, name: cf.connected_pipe.name, type: "pipe" },
          via: cf.label,
          field_id: cf.field_id,
          automation_id: null,
        });
      }
    }

    for (const tf of structure.table_fields) {
      if (tf.connected_table) {
        connections.push({
          type: "pipe_to_database",
          source: { id: pipe.id, name: pipe.name, type: "pipe" },
          target: { id: tf.connected_table.id, name: tf.connected_table.name, type: "database" },
          via: tf.label,
          field_id: tf.field_id,
          automation_id: null,
        });
      }
    }

    for (const auto of automations) {
      for (const action of auto.actions) {
        if (action.type === "create_card" && action.pipe && action.pipe.id !== pipe.id) {
          connections.push({
            type: "automation_creates_card",
            source: { id: pipe.id, name: pipe.name, type: "pipe" },
            target: { id: action.pipe.id, name: action.pipe.name, type: "pipe" },
            via: auto.name,
            field_id: null,
            automation_id: auto.id,
          });
        }
        if (
          action.type === "create_webhook_notification" &&
          action.url &&
          (action.url.includes("make.com") || action.url.includes("hook"))
        ) {
          connections.push({
            type: "webhook_to_make",
            source: { id: pipe.id, name: pipe.name, type: "pipe" },
            target: { id: action.url, name: action.url, type: "url" },
            via: auto.name,
            field_id: null,
            automation_id: auto.id,
          });
          make_webhooks.push({
            automation_id: auto.id,
            pipe_id: pipe.id,
            url: action.url,
          });
        }
      }
    }
  }

  const uniquePipes = pipesResult.pipes.map((p) => ({ id: p.id, name: p.name }));
  const uniqueDbs = dbResult.databases.map((d) => ({ id: d.id, name: d.name }));

  const summary =
    `Organização possui ${uniquePipes.length} pipes e ${uniqueDbs.length} databases. ` +
    `Foram mapeadas ${connections.length} conexões: ` +
    `${connections.filter((c) => c.type === "pipe_to_pipe").length} pipe→pipe, ` +
    `${connections.filter((c) => c.type === "pipe_to_database").length} pipe→database, ` +
    `${connections.filter((c) => c.type === "automation_creates_card").length} automações cria card, ` +
    `${make_webhooks.length} webhooks para Make.`;

  return {
    org_id: resolvedOrgId,
    pipes: uniquePipes,
    databases: uniqueDbs,
    connections,
    make_webhooks,
    summary,
  };
}

async function mapPipeDependencies(pipe_id: string) {
  const [structure, automations] = await Promise.all([
    getPipeStructure(pipe_id),
    getPipeAutomations(pipe_id),
  ]);

  const directPipes = new Set<string>();
  const directDbs = new Set<string>();
  const pipeNames = new Map<string, string>();
  const dbNames = new Map<string, string>();

  for (const cf of structure.connection_fields) {
    if (cf.connected_pipe) {
      directPipes.add(cf.connected_pipe.id);
      pipeNames.set(cf.connected_pipe.id, cf.connected_pipe.name);
    }
  }
  for (const tf of structure.table_fields) {
    if (tf.connected_table) {
      directDbs.add(tf.connected_table.id);
      dbNames.set(tf.connected_table.id, tf.connected_table.name);
    }
  }
  for (const auto of automations) {
    for (const action of auto.actions) {
      if (action.type === "create_card" && action.pipe) {
        directPipes.add(action.pipe.id);
        pipeNames.set(action.pipe.id, action.pipe.name);
      }
    }
  }

  const indirectPipes = new Set<string>();
  const indirectDbs = new Set<string>();

  for (const pid of directPipes) {
    try {
      const [s2, a2] = await Promise.all([
        getPipeStructure(pid),
        getPipeAutomations(pid),
      ]);
      for (const cf of s2.connection_fields) {
        if (cf.connected_pipe && cf.connected_pipe.id !== pipe_id && !directPipes.has(cf.connected_pipe.id)) {
          indirectPipes.add(cf.connected_pipe.id);
          pipeNames.set(cf.connected_pipe.id, cf.connected_pipe.name);
        }
      }
      for (const tf of s2.table_fields) {
        if (tf.connected_table && !directDbs.has(tf.connected_table.id)) {
          indirectDbs.add(tf.connected_table.id);
          dbNames.set(tf.connected_table.id, tf.connected_table.name);
        }
      }
      for (const auto of a2) {
        for (const action of auto.actions) {
          if (action.type === "create_card" && action.pipe && action.pipe.id !== pipe_id && !directPipes.has(action.pipe.id)) {
            indirectPipes.add(action.pipe.id);
            pipeNames.set(action.pipe.id, action.pipe.name);
          }
        }
      }
    } catch {
      // skip inaccessible pipes
    }
  }

  const toList = (ids: Set<string>, names: Map<string, string>) =>
    [...ids].map((id) => ({ id, name: names.get(id) ?? id }));

  return {
    pipe: { id: pipe_id, name: structure.name },
    direct_connections: {
      pipes: toList(directPipes, pipeNames),
      databases: toList(directDbs, dbNames),
    },
    indirect_connections: {
      pipes: toList(indirectPipes, pipeNames),
      databases: toList(indirectDbs, dbNames),
    },
    all_involved_pipes: toList(new Set([...directPipes, ...indirectPipes]), pipeNames),
    all_involved_databases: toList(new Set([...directDbs, ...indirectDbs]), dbNames),
    depth_map: {
      depth_0: [{ id: pipe_id, name: structure.name }],
      depth_1: toList(directPipes, pipeNames),
      depth_2: toList(indirectPipes, pipeNames),
    },
  };
}

async function analyzeFieldDependencies(pipe_id: string) {
  const structure = await getPipeStructure(pipe_id);

  interface DepEntry {
    field_id: string;
    field_label: string;
    field_type: string;
    phase: string;
    controls: string[];
    controlled_by: string[];
    external_dependency: boolean;
  }

  const depMap = new Map<string, DepEntry>();

  const allFieldsWithPhase = [
    ...( (structure.start_form_fields ?? []).map((f) => ({ ...f, phase_name: "Start Form" })) ),
    ...structure.phases.flatMap((ph) =>
      (ph.fields ?? []).map((f) => ({ ...f, phase_name: ph.name }))
    ),
  ];

  for (const f of allFieldsWithPhase) {
    if (!depMap.has(f.id)) {
      depMap.set(f.id, {
        field_id: f.id,
        field_label: f.label,
        field_type: f.type,
        phase: f.phase_name,
        controls: [],
        controlled_by: [],
        external_dependency: !!(f.connectedPipe || f.connectedTable),
      });
    }
  }

  let conditionalCount = 0;
  let externalCount = 0;

  for (const f of allFieldsWithPhase) {
    if (f.conditions && f.conditions.length > 0) {
      conditionalCount++;
      for (const cond of f.conditions) {
        for (const expr of cond.condition_expressions) {
          const controller = depMap.get(expr.field.id);
          const controlled = depMap.get(f.id);
          if (controller) controller.controls.push(f.label);
          if (controlled) controlled.controlled_by.push(expr.field.label);
        }
      }
    }
    if (f.connectedPipe || f.connectedTable) externalCount++;
  }

  const chains: string[] = [];
  for (const entry of depMap.values()) {
    if (entry.controls.length > 0) {
      for (const controlled of entry.controls) {
        chains.push(`"${entry.field_label}" controla "${controlled}"`);
      }
    }
  }

  return {
    pipe_id,
    pipe_name: structure.name,
    field_dependency_map: [...depMap.values()],
    conditional_fields_count: conditionalCount,
    external_dependencies_count: externalCount,
    dependency_chains: chains,
    summary: `Pipe "${structure.name}" tem ${depMap.size} campos. ${conditionalCount} campos com condições. ${externalCount} dependências externas (conexões com pipes ou databases). ${chains.length} relações de controle mapeadas.`,
  };
}

// ── Group 3: Make Bridge ──────────────────────────────────────────────────────

type MakeOperation =
  | "get_card"
  | "list_cards"
  | "create_card"
  | "move_card"
  | "update_field"
  | "get_database_record"
  | "create_database_record";

async function generateMakeQuery(
  pipe_id: string,
  operation: MakeOperation,
  options?: object
) {
  const structure = await getPipeStructure(pipe_id);
  const phases = structure.phases;
  const firstPhase = phases[0] ?? { id: "PHASE_ID", name: "Primeira fase" };
  const allFields = [
    ...(structure.start_form_fields ?? []),
    ...phases.flatMap((ph) => ph.fields ?? []),
  ];

  type MakeHttpConfig = {
    method: string;
    url: string;
    headers: Array<{ name: string; value: string }>;
    body_type: string;
    body: string;
  };

  type OutputMapping = Array<{ make_expression: string; description: string }>;

  let graphql_query = "";
  let make_http_config: MakeHttpConfig = {
    method: "POST",
    url: "https://api.pipefy.com/graphql",
    headers: [
      { name: "Authorization", value: "Bearer SEU_TOKEN_PIPEFY" },
      { name: "Content-Type", value: "application/json" },
    ],
    body_type: "raw",
    body: "",
  };
  let make_output_mapping: OutputMapping = [];

  const fieldsList = allFields
    .slice(0, 10)
    .map((f) => `            { fieldId: "${f.id}", value: "{{N.${f.label.toLowerCase().replace(/\s/g, "_")}}}" }`)
    .join("\n");

  const fieldsQuery = allFields
    .slice(0, 10)
    .map((f) => `        ${f.id}: ${f.label}`)
    .join("\n");

  switch (operation) {
    case "get_card":
      graphql_query = `query GetCard($cardId: ID!) {\n  card(id: $cardId) {\n    id title\n    current_phase { id name }\n    fields { field { id label } value }\n  }\n}`;
      make_http_config.body = JSON.stringify({
        query: graphql_query,
        variables: { cardId: "{{1.card_id}}" },
      });
      make_output_mapping = [
        { make_expression: "{{1.data.card.id}}", description: "ID do card" },
        { make_expression: "{{1.data.card.title}}", description: "Título" },
        { make_expression: "{{1.data.card.current_phase.name}}", description: "Fase atual" },
      ];
      break;

    case "list_cards":
      graphql_query = `query ListCards($pipeId: ID!) {\n  cards(pipe_id: $pipeId, first: 50) {\n    edges { node { id title current_phase { name } } }\n  }\n}`;
      make_http_config.body = JSON.stringify({
        query: graphql_query,
        variables: { pipeId: pipe_id },
      });
      make_output_mapping = [
        { make_expression: "{{1.data.cards.edges[].node.id}}", description: "IDs dos cards" },
        { make_expression: "{{1.data.cards.edges[].node.title}}", description: "Títulos" },
      ];
      break;

    case "create_card":
      graphql_query = `mutation CreateCard($pipeId: ID!, $fields: [FieldValueInput]) {\n  createCard(input: { pipe_id: $pipeId, fields_attributes: $fields }) {\n    card { id title }\n  }\n}`;
      make_http_config.body = JSON.stringify({
        query: graphql_query,
        variables: {
          pipeId: pipe_id,
          fields: allFields.slice(0, 5).map((f) => ({
            field_id: f.id,
            field_value: `{{N.${f.label.toLowerCase().replace(/\s/g, "_")}}}`,
          })),
        },
      });
      make_output_mapping = [
        { make_expression: "{{1.data.createCard.card.id}}", description: "ID do card criado" },
        { make_expression: "{{1.data.createCard.card.title}}", description: "Título do card criado" },
      ];
      break;

    case "move_card":
      graphql_query = `mutation MoveCard($cardId: ID!, $destinationId: ID!) {\n  moveCardToPhase(input: { card_id: $cardId, destination_phase_id: $destinationId }) {\n    card { id current_phase { name } }\n  }\n}`;
      make_http_config.body = JSON.stringify({
        query: graphql_query,
        variables: {
          cardId: "{{N.card_id}}",
          destinationId: firstPhase.id,
        },
      });
      make_output_mapping = [
        { make_expression: "{{1.data.moveCardToPhase.card.id}}", description: "ID do card movido" },
        { make_expression: "{{1.data.moveCardToPhase.card.current_phase.name}}", description: "Nova fase" },
      ];
      break;

    case "update_field": {
      const firstField = allFields[0] ?? { id: "FIELD_ID", label: "campo" };
      graphql_query = `mutation UpdateField($cardId: ID!, $fieldId: ID!, $value: String!) {\n  updateCardField(input: { card_id: $cardId, field_id: $fieldId, new_value: [$value] }) {\n    card { id }\n    success\n  }\n}`;
      make_http_config.body = JSON.stringify({
        query: graphql_query,
        variables: {
          cardId: "{{N.card_id}}",
          fieldId: firstField.id,
          value: "{{N.value}}",
        },
      });
      make_output_mapping = [
        { make_expression: "{{1.data.updateCardField.success}}", description: "Atualização bem-sucedida" },
      ];
      break;
    }

    case "get_database_record":
      graphql_query = `query GetRecord($recordId: ID!) {\n  table_record(id: $recordId) {\n    id title\n    record_fields { field { id label } value }\n  }\n}`;
      make_http_config.body = JSON.stringify({
        query: graphql_query,
        variables: { recordId: "{{N.record_id}}" },
      });
      make_output_mapping = [
        { make_expression: "{{1.data.table_record.id}}", description: "ID do registro" },
        { make_expression: "{{1.data.table_record.title}}", description: "Título" },
      ];
      break;

    case "create_database_record":
      graphql_query = `mutation CreateRecord($tableId: ID!, $fields: [FieldValueInput]) {\n  createTableRecord(input: { table_id: $tableId, fields_attributes: $fields }) {\n    table_record { id title }\n  }\n}`;
      make_http_config.body = JSON.stringify({
        query: graphql_query,
        variables: {
          tableId: "TABLE_ID",
          fields: [{ field_id: "FIELD_ID", field_value: "{{N.value}}" }],
        },
      });
      make_output_mapping = [
        { make_expression: "{{1.data.createTableRecord.table_record.id}}", description: "ID do registro criado" },
      ];
      break;
  }

  // suppress unused variable warning
  void fieldsList;
  void fieldsQuery;
  void options;

  return {
    operation,
    pipe_id,
    graphql_query,
    make_http_config,
    make_output_mapping,
    instructions:
      "Cole make_http_config no módulo HTTP > Make a request do Make.com",
  };
}

async function generateWebhookPayloadSchema(
  pipe_id: string,
  trigger_type: string
) {
  const structure = await getPipeStructure(pipe_id);
  const fields = [
    ...(structure.start_form_fields ?? []),
    ...structure.phases.flatMap((ph) => ph.fields ?? []),
  ];

  const base = {
    pipe_id: { type: "string", example: pipe_id },
    card_id: { type: "string", example: "12345" },
    card_title: { type: "string", example: "Título do card" },
    fields: fields.map((f) => ({ field_id: f.id, label: f.label, value: null })),
  };

  type Schema = typeof base & {
    from_phase?: { type: string; example: string };
    to_phase?: { type: string; example: string };
    field_id?: { type: string; example: string };
    old_value?: { type: string; example: string };
    new_value?: { type: string; example: string };
    current_phase?: { type: string; example: string };
    timestamp?: { type: string; example: string };
  };

  const schema: Schema = { ...base };

  if (trigger_type === "card.move") {
    schema.from_phase = { type: "string", example: "Fase anterior" };
    schema.to_phase = { type: "string", example: "Fase destino" };
  }
  if (trigger_type === "card.field_update") {
    schema.field_id = { type: "string", example: "FIELD_ID" };
    schema.old_value = { type: "string", example: "valor anterior" };
    schema.new_value = { type: "string", example: "novo valor" };
  }
  if (["card.done", "card.expired", "card.late"].includes(trigger_type)) {
    schema.current_phase = { type: "string", example: "Fase final" };
    schema.timestamp = { type: "string", example: new Date().toISOString() };
  }

  const make_variables = [
    { expression: "{{1.card_id}}", description: "ID do card" },
    { expression: "{{1.card_title}}", description: "Título do card" },
    { expression: "{{1.pipe_id}}", description: "ID do pipe" },
    ...fields.slice(0, 8).map((f) => ({
      expression: `{{1.fields[].value}}`,
      description: `Valor do campo "${f.label}"`,
    })),
  ];

  return {
    trigger: trigger_type,
    payload_schema: schema,
    make_variables,
    instructions:
      "Estas são as variáveis disponíveis no módulo Webhook do Make após receber este evento",
  };
}

// ── Group 4: Construction ─────────────────────────────────────────────────────

async function createPipe(org_id: string, name: string, description?: string) {
  const data = await graphql(
    `mutation($input: CreatePipeInput!) {
      createPipe(input: $input) {
        pipe { id name }
      }
    }`,
    {
      input: {
        organization_id: org_id,
        name,
        ...(description ? { description } : {}),
      },
    }
  );
  return {
    ...data.createPipe.pipe,
    message: "Pipe criado. Use create_phase para adicionar fases.",
  };
}

async function createPhase(
  pipe_id: string,
  name: string,
  description?: string
) {
  const data = await graphql(
    `mutation($input: CreatePhaseInput!) {
      createPhase(input: $input) {
        phase { id name }
      }
    }`,
    {
      input: {
        pipe_id,
        name,
        ...(description ? { description } : {}),
      },
    }
  );
  return { ...data.createPhase.phase, pipe_id };
}

async function createField(
  phase_id: string,
  label: string,
  type: string,
  required?: boolean,
  options?: string[],
  connected_pipe_id?: string,
  connected_table_id?: string
) {
  const input: Record<string, unknown> = { phase_id, label, type };
  if (required !== undefined) input.required = required;
  if (options && options.length > 0) input.options = options;
  if (connected_pipe_id) input.connectedPipeId = connected_pipe_id;
  if (connected_table_id) input.tableId = connected_table_id;

  const data = await graphql(
    `mutation($input: CreatePhaseFieldInput!) {
      createPhaseField(input: $input) {
        phase_field { id label type }
      }
    }`,
    { input }
  );
  return { ...data.createPhaseField.phase_field, phase_id };
}

async function createStartFormField(
  pipe_id: string,
  label: string,
  type: string,
  required?: boolean,
  options?: string[]
) {
  const input: Record<string, unknown> = { pipe_id, label, type };
  if (required !== undefined) input.required = required;
  if (options && options.length > 0) input.options = options;

  const data = await graphql(
    `mutation($input: CreateStartFormFieldInput!) {
      createStartFormField(input: $input) {
        start_form_field { id label type }
      }
    }`,
    { input }
  );
  return { ...data.createStartFormField.start_form_field, pipe_id };
}

async function updatePhase(
  phase_id: string,
  updates: { name?: string; description?: string }
) {
  const data = await graphql(
    `mutation($input: UpdatePhaseInput!) {
      updatePhase(input: $input) {
        phase { id name }
      }
    }`,
    { input: { id: phase_id, ...updates } }
  );
  return { ...data.updatePhase.phase, updated: true };
}

async function updateField(
  field_id: string,
  updates: { label?: string; required?: boolean; options?: string[] }
) {
  const data = await graphql(
    `mutation($input: UpdatePhaseFieldInput!) {
      updatePhaseField(input: $input) {
        phase_field { id label }
      }
    }`,
    { input: { id: field_id, ...updates } }
  );
  return { ...data.updatePhaseField.phase_field, updated: true };
}

// ── Group 5: Automations ──────────────────────────────────────────────────────

async function getValidAutomationTypes() {
  const hardcoded = {
    triggers: [
      "card.create",
      "card.move",
      "card.done",
      "card.expired",
      "card.late",
      "card.field_update",
    ],
    operators: [
      "eq", "neq", "gt", "lt", "gte", "lte",
      "contains", "not_contains", "is_empty", "is_not_empty",
    ],
    actions: [
      "move_card",
      "update_field",
      "create_card",
      "send_email",
      "create_webhook_notification",
      "set_due_date",
      "assign_card",
    ],
    note: "Use estes tipos exatos ao chamar create_automation — tipos inválidos causam erro silencioso",
  };

  try {
    const data = await graphql(
      `{ pipeAutomationTriggers { type label } pipeAutomationActions { type label } }`
    );
    return {
      triggers: data.pipeAutomationTriggers,
      actions: data.pipeAutomationActions,
      operators: hardcoded.operators,
      note: hardcoded.note,
    };
  } catch {
    // try introspection
    try {
      const introData = await introspectSchema();
      const relevant = (introData.types as Array<{ name: string }>).filter((t) =>
        t.name.toLowerCase().includes("automation") ||
        t.name.toLowerCase().includes("trigger") ||
        t.name.toLowerCase().includes("action")
      );
      if (relevant.length > 0) {
        return { ...hardcoded, schema_types: relevant };
      }
    } catch {
      // fall through
    }
    return hardcoded;
  }
}

async function createAutomation(
  pipe_id: string,
  name: string,
  trigger: object,
  conditions?: object[],
  actions: object[] = []
) {
  await getValidAutomationTypes();

  const input: Record<string, unknown> = { pipe_id, name, trigger, actions };
  if (conditions && conditions.length > 0) input.conditions = conditions;

  const data = await graphql(
    `mutation($input: CreateAutomationInput!) {
      createAutomation(input: $input) {
        automation {
          id name active
          trigger { type }
          actions { type url }
        }
      }
    }`,
    { input }
  );

  const auto = data.createAutomation.automation;
  const webhookUrl = auto.actions?.find(
    (a: { type: string; url?: string }) => a.url
  )?.url ?? null;

  return {
    id: auto.id,
    name: auto.name,
    active: auto.active,
    trigger_type: auto.trigger?.type ?? null,
    actions_count: auto.actions?.length ?? 0,
    webhook_url: webhookUrl,
  };
}

async function getAutomationDetail(automation_id: string, pipe_id: string) {
  const automations = await getPipeAutomations(pipe_id);
  const auto = automations.find((a) => a.id === automation_id);

  if (!auto) throw new Error(`Automação ${automation_id} não encontrada no pipe ${pipe_id}`);

  const triggerHuman = (() => {
    const t = auto.trigger.type;
    if (t === "card.create") return "Quando um card é criado";
    if (t === "card.move") return `Quando um card é movido${auto.trigger.phase ? ` para a fase "${auto.trigger.phase.name}"` : ""}`;
    if (t === "card.done") return "Quando um card é finalizado";
    if (t === "card.expired") return "Quando um card expira";
    if (t === "card.late") return "Quando um card fica em atraso";
    if (t === "card.field_update") return `Quando o campo "${auto.trigger.field?.label ?? "?"}" é atualizado`;
    return t;
  })();

  const conditionsHuman = (auto.trigger.conditions ?? []).map((c) => {
    return `SE o campo "${c.field_id}" ${c.operator} "${c.value}"`;
  });

  const actionsHuman = auto.actions.map((a) => {
    if (a.type === "move_card") return `ENTÃO mover card para a fase "${a.phase?.name ?? "?"}"`;
    if (a.type === "update_field") return `ENTÃO atualizar campo "${a.field?.label ?? "?"}" para "${a.value ?? "?"}"`;
    if (a.type === "create_card") return `ENTÃO criar card no pipe "${a.pipe?.name ?? "?"}"`;
    if (a.type === "send_email") return `ENTÃO enviar e-mail`;
    if (a.type === "create_webhook_notification") return `ENTÃO enviar webhook para "${a.url ?? "?"}"`;
    if (a.type === "set_due_date") return `ENTÃO definir prazo`;
    if (a.type === "assign_card") return `ENTÃO atribuir card`;
    return `ENTÃO ${a.type}`;
  });

  return {
    id: auto.id,
    name: auto.name,
    active: auto.active,
    classification: auto.classification,
    trigger_human: triggerHuman,
    conditions_human: conditionsHuman,
    actions_human: actionsHuman,
    make_webhook_url: auto.make_webhook_url,
    raw: auto,
  };
}

// ── Group 6: Dynamic Exploration ──────────────────────────────────────────────

async function introspectSchema(type_name?: string) {
  const now = Date.now();
  if (schemaCache && now - schemaCacheTime < SCHEMA_CACHE_TTL) {
    const types = type_name
      ? filterSchemaTypes(schemaCache, type_name)
      : summarizeTypes(schemaCache);
    return {
      cached: true,
      types_count: schemaCache.length,
      types,
      instruction:
        "Use build_and_run_query para construir queries baseadas neste schema",
    };
  }

  const data = await graphql(`
    query {
      __schema {
        types {
          name kind description
          fields(includeDeprecated: false) {
            name description
            type { name kind ofType { name kind ofType { name kind } } }
            args { name description type { name kind ofType { name kind } } defaultValue }
          }
          inputFields {
            name description
            type { name kind ofType { name kind } }
            defaultValue
          }
          enumValues { name description }
        }
      }
    }
  `);

  const allTypes = (
    data.__schema.types as Array<{ name: string; kind: string; description: string }>
  ).filter((t) => !t.name.startsWith("__"));

  schemaCache = allTypes;
  schemaCacheTime = now;

  const types = type_name
    ? filterSchemaTypes(allTypes, type_name)
    : summarizeTypes(allTypes);

  return {
    cached: false,
    types_count: allTypes.length,
    types,
    instruction:
      "Use build_and_run_query para construir queries baseadas neste schema",
  };
}

function summarizeTypes(types: Array<{ name: string; kind: string; description: string }>) {
  return types.map((t) => ({ name: t.name, kind: t.kind, description: t.description }));
}

function filterSchemaTypes(
  types: Array<{ name: string; kind: string; description: string }>,
  type_name: string
) {
  const norm = type_name.toLowerCase();
  return types.filter((t) => t.name.toLowerCase().includes(norm));
}

async function buildAndRunQuery(
  intent: string,
  variables?: object,
  pipe_id?: string
) {
  const schemaResult = await introspectSchema();

  let pipe_context = null;
  if (pipe_id) {
    try {
      pipe_context = await getPipeStructure(pipe_id);
    } catch {
      // ignore
    }
  }

  const keywords = intent
    .toLowerCase()
    .split(/\s+/)
    .filter((t) => t.length >= 4);

  const schema_context = (
    schemaResult.types as Array<{ name: string; kind: string; description: string }>
  ).filter((t) => {
    const text = `${t.name} ${t.description ?? ""}`.toLowerCase();
    return keywords.some((k) => text.includes(k));
  });

  return {
    schema_context,
    pipe_context,
    intent,
    instruction: `Com base no schema_context e pipe_context acima, construa a GraphQL query para: ${intent}. Depois chame call_pipefy_api com a query construída.`,
    variables_hint: variables ?? {},
  };
}

async function callPipefyApi(query: string, variables?: object) {
  try {
    const data = await graphql(query, variables);
    return { result: data };
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return {
      errors: [{ message, suggestion: "Verifique a sintaxe da query com introspect_schema" }],
    };
  }
}

// ── Group 7: Cards ────────────────────────────────────────────────────────────

async function listCards(pipe_id: string, phase_id?: string, limit = 30, search?: string) {
  let cards: unknown[];

  if (phase_id) {
    const data = await graphql(
      `query($phaseId: ID!, $limit: Int!) {
        phase(id: $phaseId) {
          id name
          cards(first: $limit) {
            edges {
              node {
                id title created_at updated_at
                assignees { id name email }
                fields { field { id label type } value }
              }
            }
          }
        }
      }`,
      { phaseId: phase_id, limit }
    );
    cards = (data.phase.cards?.edges ?? []).map(
      (e: { node: Record<string, unknown> }) => ({
        ...e.node,
        current_phase: { id: phase_id, name: data.phase.name },
      })
    );
  } else {
    const data = await graphql(
      `query($pipeId: ID!, $limit: Int!) {
        cards(pipe_id: $pipeId, first: $limit) {
          edges {
            node {
              id title created_at updated_at
              current_phase { id name }
              assignees { id name email }
              fields { field { id label type } value }
            }
          }
        }
      }`,
      { pipeId: pipe_id, limit }
    );
    cards = (data.cards?.edges ?? []).map((e: { node: unknown }) => e.node);
  }

  if (search) {
    const q = search.toLowerCase();
    cards = (cards as Array<{ title?: string }>).filter((c) =>
      c.title?.toLowerCase().includes(q)
    );
  }

  return {
    pipe_id,
    phase_filter: phase_id ?? null,
    search_filter: search ?? null,
    total_returned: cards.length,
    cards,
  };
}

// ── Group 8: Webhook Logs ─────────────────────────────────────────────────────

async function getWebhookDispatchLogs(pipe_id: string) {
  const [structure, automations] = await Promise.all([
    getPipeStructure(pipe_id),
    getPipeAutomations(pipe_id),
  ]);

  const webhookAutomations = automations.filter((a) => a.classification === "webhook");
  const makeAutomations = automations.filter(
    (a) => a.make_webhook_url && a.make_webhook_url.length > 0
  );

  // Attempt to get recent webhook delivery status via pipe webhooks
  // Pipefy's public API doesn't expose per-delivery logs, so we expose
  // what's available: webhook config + automations that dispatch them.
  const configuredWebhooks = structure.webhooks ?? [];

  // Try extended webhook query for any available log data
  let webhookDetails: unknown[] = [];
  try {
    const data = await graphql(
      `query($id: ID!) {
        pipe(id: $id) {
          webhooks { id name url actions active }
        }
      }`,
      { id: pipe_id }
    );
    webhookDetails = data.pipe?.webhooks ?? [];
  } catch { /* ignora */ }

  return {
    pipe_id,
    pipe_name: structure.name,
    configured_webhooks: configuredWebhooks.map((w) => ({
      id: w.id,
      name: w.name,
      url: w.url,
      events: w.actions,
      active: w.active,
      note: "Webhook configurado diretamente no pipe (recebe eventos externos)",
    })),
    automations_dispatching_webhooks: webhookAutomations.map((a) => ({
      id: a.id,
      name: a.name,
      active: a.active,
      trigger: a.trigger,
      target_urls: a.actions
        .filter((act) => act.type === "create_webhook_notification")
        .map((act) => act.url),
      is_make_integration: (a.make_webhook_url?.length ?? 0) > 0,
      make_urls: a.make_webhook_url ?? [],
    })),
    make_integrations_count: makeAutomations.length,
    summary:
      `Pipe "${structure.name}": ${configuredWebhooks.length} webhook(s) configurado(s), ` +
      `${webhookAutomations.length} automação(ões) disparam webhooks ` +
      `(${makeAutomations.length} apontam para Make.com). ` +
      "Logs detalhados de entrega não estão disponíveis via API pública do Pipefy — " +
      "para rastrear falhas de entrega, use correlate_pipefy_event no Make-MCP com o card_id suspeito.",
    webhook_details: webhookDetails,
  };
}

// ── Group 9: Documentation ────────────────────────────────────────────────────

async function documentFlow(pipe_id: string) {
  const [structure, automations] = await Promise.all([
    getPipeStructure(pipe_id),
    getPipeAutomations(pipe_id),
  ]);

  const allFields = [
    ...(structure.start_form_fields ?? []),
    ...structure.phases.flatMap((ph) => ph.fields ?? []),
  ];
  const pipeConnections = allFields.filter((f) => f.connectedPipe);
  const dbConnections = allFields.filter((f) => f.connectedTable);
  const makeAutomations = automations.filter(
    (a) => a.make_webhook_url && a.make_webhook_url.length > 0
  );
  const totalCards = structure.phases.reduce((sum, p) => sum + (p.cards_count ?? 0), 0);

  // ── Build markdown ──────────────────────────────────────────────────────────
  let doc = `# Fluxo: ${structure.name}\n\n`;
  if (structure.description) doc += `**Descrição:** ${structure.description}\n\n`;
  doc += `**Status:** ${structure.status ?? "ativo"} | **Cards em andamento:** ${totalCards}\n\n`;
  doc += `---\n\n`;

  // Formulário de abertura
  if ((structure.start_form_fields ?? []).length > 0) {
    doc += `## Formulário de Abertura\n\n`;
    for (const f of structure.start_form_fields ?? []) {
      const req = f.required ? " *(obrigatório)*" : "";
      const ext = f.connectedPipe
        ? ` → Pipe: **${f.connectedPipe.name}**`
        : f.connectedTable
        ? ` → Database: **${f.connectedTable.name}**`
        : "";
      doc += `- **${f.label}** \`${f.type}\`${req}${ext}\n`;
    }
    doc += "\n";
  }

  // Fases
  doc += `## Fases\n\n`;
  for (const phase of structure.phases) {
    const count = phase.cards_count !== undefined ? ` *(${phase.cards_count} cards)*` : "";
    doc += `### ${phase.name}${count}\n`;
    if (phase.description) doc += `> ${phase.description}\n\n`;
    if ((phase.fields ?? []).length > 0) {
      for (const f of phase.fields ?? []) {
        const req = f.required ? " *(obrigatório)*" : "";
        const ext = f.connectedPipe
          ? ` → Pipe: **${f.connectedPipe.name}**`
          : f.connectedTable
          ? ` → Database: **${f.connectedTable.name}**`
          : "";
        const cond = (f.conditions ?? []).length > 0 ? " *(campo condicional)*" : "";
        doc += `- **${f.label}** \`${f.type}\`${req}${ext}${cond}\n`;
      }
    } else {
      doc += `*Sem campos configurados nesta fase.*\n`;
    }
    doc += "\n";
  }

  // Conexões
  if (pipeConnections.length > 0 || dbConnections.length > 0) {
    doc += `## Conexões\n\n`;
    if (pipeConnections.length > 0) {
      doc += `### Pipes Conectados\n`;
      for (const f of pipeConnections) {
        doc += `- Campo **${f.label}** conecta ao pipe **${f.connectedPipe!.name}** (ID: \`${f.connectedPipe!.id}\`)\n`;
      }
      doc += "\n";
    }
    if (dbConnections.length > 0) {
      doc += `### Databases Conectados\n`;
      for (const f of dbConnections) {
        doc += `- Campo **${f.label}** conecta ao database **${f.connectedTable!.name}** (ID: \`${f.connectedTable!.id}\`)\n`;
      }
      doc += "\n";
    }
  }

  // Automações
  if (automations.length > 0) {
    doc += `## Automações (${automations.length})\n\n`;
    for (const auto of automations) {
      const status = auto.active ? "ativa" : "**inativa**";
      doc += `### ${auto.name} — ${status} \`${auto.classification}\`\n\n`;

      const t = auto.trigger.type;
      let triggerText =
        t === "card.create" ? "Quando um card é criado" :
        t === "card.move" ? `Quando um card é movido${auto.trigger.phase ? ` para a fase **"${auto.trigger.phase.name}"**` : ""}` :
        t === "card.done" ? "Quando um card é finalizado (movido para fase final)" :
        t === "card.expired" ? "Quando um card expira (SLA vencido)" :
        t === "card.late" ? "Quando um card fica em atraso" :
        t === "card.field_update" ? `Quando o campo **"${auto.trigger.field?.label ?? "?"}"** é atualizado` :
        t;

      doc += `**Trigger:** ${triggerText}\n`;

      const conditions = auto.trigger.conditions ?? [];
      if (conditions.length > 0) {
        doc += `**Condições:**\n`;
        for (const c of conditions) {
          doc += `- Campo \`${c.field_id}\` ${c.operator} \`"${c.value}"\`\n`;
        }
      }

      doc += `**Ações:**\n`;
      for (const action of auto.actions) {
        if (action.type === "move_card")
          doc += `- Mover card para a fase **"${action.phase?.name ?? "?"}"**\n`;
        else if (action.type === "update_field")
          doc += `- Atualizar campo **"${action.field?.label ?? "?"}"** para \`"${action.value ?? "?"}"\`\n`;
        else if (action.type === "create_card")
          doc += `- Criar card no pipe **${action.pipe?.name ?? "?"}** (ID: \`${action.pipe?.id ?? "?"}\`)\n`;
        else if (action.type === "send_email")
          doc += `- Enviar e-mail\n`;
        else if (action.type === "create_webhook_notification") {
          const isMake =
            action.url &&
            (action.url.includes("make.com") || action.url.includes("hook.eu") || action.url.includes("hook.us"));
          doc += `- Disparar webhook para \`${action.url ?? "?"}\`${isMake ? " *(Make.com)*" : ""}\n`;
        } else if (action.type === "set_due_date")
          doc += `- Definir prazo do card\n`;
        else if (action.type === "assign_card")
          doc += `- Atribuir card a membro\n`;
        else
          doc += `- \`${action.type}\`\n`;
      }
      doc += "\n";
    }
  }

  // Integração Make
  if (makeAutomations.length > 0) {
    doc += `## Integrações com Make.com\n\n`;
    doc += `Este pipe possui **${makeAutomations.length}** automação(ões) que disparam webhooks Make:\n\n`;
    for (const auto of makeAutomations) {
      doc += `| Automação | Trigger | URL Make |\n|---|---|---|\n`;
      doc += `| ${auto.name} | \`${auto.trigger.type}\` | \`${(auto.make_webhook_url ?? []).join(", ")}\` |\n\n`;
    }
    doc += `> Para documentar o cenário Make completo, use \`get_scenario\` no Make-MCP com os IDs dos cenários correspondentes.\n\n`;
  }

  // Webhooks de entrada
  if ((structure.webhooks ?? []).length > 0) {
    doc += `## Webhooks de Entrada\n\n`;
    for (const wh of structure.webhooks ?? []) {
      doc += `- **${wh.name}** (${wh.active ? "ativo" : "inativo"}): \`${wh.url}\` — eventos: \`${wh.actions.join(", ")}\`\n`;
    }
    doc += "\n";
  }

  // Membros
  if ((structure.members ?? []).length > 0) {
    doc += `## Membros (${structure.members!.length})\n\n`;
    for (const m of structure.members!) {
      doc += `- **${m.user.name}** (${m.user.email}) — \`${m.role}\`\n`;
    }
    doc += "\n";
  }

  return {
    pipe_id,
    pipe_name: structure.name,
    summary: {
      phases: structure.phases.length,
      total_fields: allFields.length,
      total_cards_in_progress: totalCards,
      automations: automations.length,
      make_integrations: makeAutomations.length,
      connections_to_pipes: pipeConnections.length,
      connections_to_databases: dbConnections.length,
      webhooks_inbound: (structure.webhooks ?? []).length,
    },
    documentation_markdown: doc,
  };
}

async function getPipeMetrics(pipe_id: string) {
  const structure = await getPipeStructure(pipe_id);

  const data = await graphql(
    `query($pipeId: ID!) {
      cards(pipe_id: $pipeId, first: 50) {
        edges {
          node {
            id title created_at finished_at
            current_phase { id name }
            assignees { id name }
          }
        }
      }
    }`,
    { pipeId: pipe_id }
  );

  const cards = (data.cards?.edges ?? []).map((e: { node: unknown }) => e.node) as Array<{
    id: string;
    title: string;
    created_at: string;
    finished_at: string | null;
    current_phase: { id: string; name: string };
    assignees: Array<{ id: string; name: string }>;
  }>;

  const phaseDistribution = structure.phases.map((ph) => ({
    phase_id: ph.id,
    phase_name: ph.name,
    cards_count: ph.cards_count ?? 0,
  }));
  const totalInProgress = phaseDistribution.reduce((sum, p) => sum + p.cards_count, 0);

  const byDay: Record<string, number> = {};
  for (const card of cards) {
    const day = card.created_at?.substring(0, 10) ?? "unknown";
    byDay[day] = (byDay[day] ?? 0) + 1;
  }
  const volumeByDay = Object.entries(byDay)
    .sort((a, b) => a[0].localeCompare(b[0]))
    .map(([date, count]) => ({ date, count }));

  const finishedCards = cards.filter((c) => c.finished_at);
  const completionRate = cards.length > 0 ? Math.round((finishedCards.length / cards.length) * 100) : 0;

  const bottleneck = phaseDistribution.reduce(
    (max, p) => (p.cards_count > (max?.cards_count ?? 0) ? p : max),
    phaseDistribution[0] ?? { phase_name: "N/A", cards_count: 0, phase_id: "" }
  );

  const byAssignee: Record<string, number> = {};
  for (const card of cards) {
    for (const a of card.assignees ?? []) {
      byAssignee[a.name] = (byAssignee[a.name] ?? 0) + 1;
    }
  }
  const topAssignees = Object.entries(byAssignee)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 5)
    .map(([name, count]) => ({ name, count }));

  return {
    pipe_id,
    pipe_name: structure.name,
    snapshot: {
      total_cards_in_progress: totalInProgress,
      phases: phaseDistribution,
      bottleneck_phase: bottleneck,
    },
    from_recent_sample: {
      cards_analyzed: cards.length,
      completion_rate: `${completionRate}%`,
      finished_cards: finishedCards.length,
      in_progress_cards: cards.length - finishedCards.length,
      volume_by_day: volumeByDay,
      top_assignees: topAssignees,
    },
    note: "Métricas baseadas nos últimos 50 cards. Para análise por fase específica, use list_cards com phase_id.",
  };
}

async function mapPipefyMakeIntegration(pipe_id: string) {
  const [structure, automations] = await Promise.all([
    getPipeStructure(pipe_id),
    getPipeAutomations(pipe_id),
  ]);

  const allFields = [
    ...(structure.start_form_fields ?? []),
    ...structure.phases.flatMap((ph) => ph.fields ?? []),
  ];

  // Outgoing: automations that fire webhooks
  const outgoing = automations
    .filter((a) => a.classification === "webhook")
    .map((a) => {
      const webhookActions = a.actions.filter((act) => act.type === "create_webhook_notification");
      return {
        automation_id: a.id,
        automation_name: a.name,
        active: a.active,
        trigger: {
          type: a.trigger.type,
          phase: a.trigger.phase?.name ?? null,
          field: a.trigger.field?.label ?? null,
          conditions_count: (a.trigger.conditions ?? []).length,
        },
        dispatches_to: webhookActions.map((act) => {
          const url = act.url ?? "";
          const isMake =
            url.includes("make.com") ||
            url.includes("hook.eu") ||
            url.includes("hook.us") ||
            url.includes("integromat");
          return { url, is_make: isMake };
        }),
        data_sent: `card_id, card_title, pipe_id${a.trigger.type === "card.move" ? ", from_phase, to_phase" : ""}${a.trigger.type === "card.field_update" ? ", field_id, old_value, new_value" : ""} + todos os campos do card`,
      };
    });

  // Incoming: webhooks registered on the pipe
  const incoming = (structure.webhooks ?? []).map((w) => ({
    webhook_id: w.id,
    name: w.name,
    url: w.url,
    listens_for: w.actions,
    active: w.active,
    purpose: "Endpoint para receber chamadas externas (ex: Make envia dados de volta para este pipe via Pipefy API)",
  }));

  // Data connections
  const connectedPipes = allFields
    .filter((f) => f.connectedPipe)
    .map((f) => ({ field_id: f.id, field_label: f.label, connected_pipe_id: f.connectedPipe!.id, connected_pipe_name: f.connectedPipe!.name }));
  const connectedDatabases = allFields
    .filter((f) => f.connectedTable)
    .map((f) => ({ field_id: f.id, field_label: f.label, connected_db_id: f.connectedTable!.id, connected_db_name: f.connectedTable!.name }));

  // Payload schemas for each outgoing trigger type
  const triggerTypes = [...new Set(outgoing.map((o) => o.trigger.type))];
  const payloadSchemas = triggerTypes.map((triggerType) => ({
    trigger_type: triggerType,
    make_variables: [
      { expression: "{{1.card_id}}", description: "ID do card Pipefy" },
      { expression: "{{1.card_title}}", description: "Título do card" },
      { expression: "{{1.pipe_id}}", description: `ID do pipe (${pipe_id})` },
      ...(triggerType === "card.move" ? [
        { expression: "{{1.from_phase}}", description: "Fase de origem" },
        { expression: "{{1.to_phase}}", description: "Fase de destino" },
      ] : []),
      ...(triggerType === "card.field_update" ? [
        { expression: "{{1.field_id}}", description: "ID do campo atualizado" },
        { expression: "{{1.old_value}}", description: "Valor anterior" },
        { expression: "{{1.new_value}}", description: "Novo valor" },
      ] : []),
    ],
  }));

  const makeUrls = outgoing.flatMap((o) => o.dispatches_to.filter((d) => d.is_make).map((d) => d.url));

  return {
    pipe_id,
    pipe_name: structure.name,
    integration_summary: {
      outgoing_webhook_automations: outgoing.length,
      outgoing_to_make: outgoing.filter((o) => o.dispatches_to.some((d) => d.is_make)).length,
      incoming_webhooks: incoming.length,
      connected_pipes: connectedPipes.length,
      connected_databases: connectedDatabases.length,
      make_webhook_urls_found: makeUrls,
    },
    outgoing_to_make: outgoing,
    incoming_webhooks: incoming,
    data_connections: { pipes: connectedPipes, databases: connectedDatabases },
    webhook_payload_schemas: payloadSchemas,
    make_side_instructions:
      makeUrls.length > 0
        ? `Para completar o mapa de integração, no Make-MCP: (1) use search_scenarios para encontrar cenários com essas URLs como trigger, (2) use get_scenario para ver o blueprint completo e (3) use document_scenario para gerar documentação legível do cenário.`
        : `Nenhum webhook Make.com identificado nas automações deste pipe. Verifique se as URLs de webhook estão configuradas corretamente nas automações.`,
  };
}

async function analyzePipeHealth(pipe_id: string) {
  const [structure, automations] = await Promise.all([
    getPipeStructure(pipe_id),
    getPipeAutomations(pipe_id),
  ]);

  let cards: Array<{
    id: string;
    created_at: string;
    finished_at: string | null;
    current_phase: { id: string; name: string };
    assignees: Array<{ id: string; name: string }>;
  }> = [];
  try {
    const data = await graphql(
      `query($pipeId: ID!) {
        cards(pipe_id: $pipeId, first: 30) {
          edges { node { id created_at finished_at current_phase { id name } assignees { id name } } }
        }
      }`,
      { pipeId: pipe_id }
    );
    cards = (data.cards?.edges ?? []).map((e: { node: unknown }) => e.node) as typeof cards;
  } catch { /* métricas não bloqueiam a análise */ }

  const issues: Array<{
    severity: "critical" | "warning" | "info";
    category: string;
    title: string;
    detail: string;
    suggestion: string;
  }> = [];

  // ── Automations ─────────────────────────────────────────────────────────────
  const makeAutomations = automations.filter((a) => (a.make_webhook_url?.length ?? 0) > 0);

  for (const a of automations.filter((a) => !a.active)) {
    const isMake = (a.make_webhook_url?.length ?? 0) > 0;
    issues.push({
      severity: isMake ? "critical" : "warning",
      category: "automação",
      title: `Automação inativa: "${a.name}"`,
      detail: `Trigger: ${a.trigger.type}${isMake ? ` — dispara webhook para Make.com (${a.make_webhook_url!.join(", ")})` : ""}.`,
      suggestion: isMake
        ? "Integração Make.com interrompida — reative ou remova se descontinuada."
        : "Verifique se a desativação foi intencional.",
    });
  }

  // Conflicting automations: same trigger+phase, contradictory move_card destinations
  const moveAutos = automations.filter(
    (a) => a.active && a.actions.some((act) => act.type === "move_card")
  );
  const triggerGroups = new Map<string, typeof moveAutos>();
  for (const auto of moveAutos) {
    const key = `${auto.trigger.type}:${auto.trigger.phase?.id ?? "any"}`;
    if (!triggerGroups.has(key)) triggerGroups.set(key, []);
    triggerGroups.get(key)!.push(auto);
  }
  for (const [, group] of triggerGroups) {
    if (group.length < 2) continue;
    const destinations = [
      ...new Set(
        group.flatMap((a) =>
          a.actions.filter((act) => act.type === "move_card").map((act) => act.phase?.name ?? "?")
        )
      ),
    ];
    if (destinations.length > 1) {
      issues.push({
        severity: "warning",
        category: "automação",
        title: `Automações conflitantes: ${group.map((a) => `"${a.name}"`).join(", ")}`,
        detail: `Mesmo trigger, mas movem cards para fases diferentes: ${destinations.join(", ")}.`,
        suggestion: "Adicione condições para que apenas uma automação execute por vez.",
      });
    }
  }

  // ── Structure ────────────────────────────────────────────────────────────────
  for (const phase of structure.phases) {
    if ((phase.fields ?? []).length === 0) {
      issues.push({
        severity: "info",
        category: "estrutura",
        title: `Fase sem campos: "${phase.name}"`,
        detail: "A fase não possui campos configurados.",
        suggestion: "Verifique se é uma fase de passagem ou se faltam campos.",
      });
    }
    const required = (phase.fields ?? []).filter((f) => f.required).length;
    const total = (phase.fields ?? []).length;
    if (total > 0 && required / total > 0.7 && required > 5) {
      issues.push({
        severity: "info",
        category: "estrutura",
        title: `Alta obrigatoriedade na fase "${phase.name}" (${required}/${total} campos)`,
        detail: `${Math.round((required / total) * 100)}% dos campos são obrigatórios.`,
        suggestion: "Alta obrigatoriedade pode causar abandono de cards. Reavalie quais são realmente obrigatórios.",
      });
    }
  }

  // ── Metrics ──────────────────────────────────────────────────────────────────
  const phaseDistrib = structure.phases.map((ph) => ({ id: ph.id, name: ph.name, count: ph.cards_count ?? 0 }));
  const totalCards = phaseDistrib.reduce((s, p) => s + p.count, 0);
  const bottleneck = phaseDistrib.reduce((max, p) => (p.count > max.count ? p : max), { id: "", name: "", count: 0 });

  if (totalCards > 0 && bottleneck.count / totalCards > 0.6) {
    issues.push({
      severity: "warning",
      category: "fluxo",
      title: `Gargalo na fase "${bottleneck.name}" (${bottleneck.count}/${totalCards} cards)`,
      detail: `${Math.round((bottleneck.count / totalCards) * 100)}% dos cards estão acumulados nesta fase.`,
      suggestion: "Investigue: campo obrigatório não preenchido, falta de automação para avançar ou sobrecarga de responsáveis.",
    });
  }

  if (cards.length > 0) {
    const unassigned = cards.filter((c) => (c.assignees ?? []).length === 0).length;
    const unassignedRate = Math.round((unassigned / cards.length) * 100);
    if (unassignedRate > 50) {
      issues.push({
        severity: "info",
        category: "operação",
        title: `${unassignedRate}% dos cards sem responsável (${unassigned}/${cards.length})`,
        detail: "Maioria dos cards não tem assignee.",
        suggestion: "Considere automação assign_card no trigger card.create para atribuição automática.",
      });
    }
  }

  if (makeAutomations.length === 0 && automations.length > 0) {
    issues.push({
      severity: "info",
      category: "integração",
      title: "Nenhuma integração Make.com detectada",
      detail: "Pipe não possui automações disparando webhooks Make.",
      suggestion: "Se integrações externas são necessárias, configure create_webhook_notification nas automações.",
    });
  }

  const allFields = [
    ...(structure.start_form_fields ?? []),
    ...structure.phases.flatMap((ph) => ph.fields ?? []),
  ];

  const critical = issues.filter((i) => i.severity === "critical").length;
  const warnings = issues.filter((i) => i.severity === "warning").length;
  const infos = issues.filter((i) => i.severity === "info").length;
  const healthScore = Math.max(0, 100 - critical * 30 - warnings * 10 - infos * 3);
  const healthLabel =
    healthScore >= 90 ? "saudável" :
    healthScore >= 70 ? "atenção" :
    healthScore >= 50 ? "problemas" : "crítico";

  return {
    pipe_id,
    pipe_name: structure.name,
    health_score: healthScore,
    health_label: healthLabel,
    issues_summary: { critical, warnings, infos, total: issues.length },
    issues,
    stats: {
      phases: structure.phases.length,
      total_fields: allFields.length,
      automations: automations.length,
      active_automations: automations.filter((a) => a.active).length,
      make_integrations: makeAutomations.length,
      total_cards_in_progress: totalCards,
    },
  };
}

async function exportPipeAsDiagramDsl(pipe_id: string) {
  const [structure, automations] = await Promise.all([
    getPipeStructure(pipe_id),
    getPipeAutomations(pipe_id),
  ]);

  const allFields = [
    ...(structure.start_form_fields ?? []),
    ...structure.phases.flatMap((ph) => ph.fields ?? []),
  ];
  const pipeConnFields = allFields.filter((f) => f.connectedPipe);
  const dbConnFields = allFields.filter((f) => f.connectedTable);
  const makeAutos = automations.filter((a) => (a.make_webhook_url?.length ?? 0) > 0);
  const createCardAutos = automations.filter((a) =>
    a.active && a.actions.some((act) => act.type === "create_card" && act.pipe)
  );

  const safe = (s: string) => s.replace(/["\[\]{}()|]/g, "").trim().substring(0, 30);

  let dsl = `flowchart LR\n`;

  // Nodes
  if ((structure.start_form_fields ?? []).length > 0) {
    dsl += `  start([\"Início\\n${safe(structure.name)}\"])\n`;
  }
  for (const ph of structure.phases) {
    const cnt = ph.cards_count !== undefined ? `\\n${ph.cards_count} cards` : "";
    dsl += `  p${ph.id}[\"${safe(ph.name)}${cnt}\"]\n`;
  }
  const seenExtPipes = new Set<string>();
  for (const f of pipeConnFields) {
    const p = f.connectedPipe!;
    if (!seenExtPipes.has(p.id)) { seenExtPipes.add(p.id); dsl += `  ep${p.id}([\"${safe(p.name)}\"])\n`; }
  }
  for (const a of createCardAutos) {
    for (const act of a.actions.filter((ac) => ac.type === "create_card" && ac.pipe)) {
      const pid = act.pipe!.id;
      if (!seenExtPipes.has(pid)) { seenExtPipes.add(pid); dsl += `  ep${pid}([\"${safe(act.pipe!.name)}\"])\n`; }
    }
  }
  const seenDbs = new Set<string>();
  for (const f of dbConnFields) {
    const db = f.connectedTable!;
    if (!seenDbs.has(db.id)) { seenDbs.add(db.id); dsl += `  db${db.id}[(\"${safe(db.name)}\")]\n`; }
  }
  if (makeAutos.length > 0) dsl += `  make{{\"Make.com\"}}\n`;

  dsl += "\n";

  // Edges
  if ((structure.start_form_fields ?? []).length > 0 && structure.phases.length > 0) {
    dsl += `  start --> p${structure.phases[0].id}\n`;
  }
  for (let i = 0; i < structure.phases.length - 1; i++) {
    dsl += `  p${structure.phases[i].id} --> p${structure.phases[i + 1].id}\n`;
  }
  const seenDbEdges = new Set<string>();
  for (const f of pipeConnFields) {
    const ph = structure.phases.find((p) => (p.fields ?? []).some((pf) => pf.id === f.id));
    if (ph) dsl += `  p${ph.id} -- \"${safe(f.label)}\" --> ep${f.connectedPipe!.id}\n`;
  }
  for (const f of dbConnFields) {
    const ph = structure.phases.find((p) => (p.fields ?? []).some((pf) => pf.id === f.id));
    if (ph) {
      const key = `${ph.id}:${f.connectedTable!.id}`;
      if (!seenDbEdges.has(key)) { seenDbEdges.add(key); dsl += `  p${ph.id} -- \"${safe(f.label)}\" --> db${f.connectedTable!.id}\n`; }
    }
  }
  for (const a of createCardAutos) {
    const fromPhase = a.trigger.phase?.id ?? structure.phases[0]?.id;
    for (const act of a.actions.filter((ac) => ac.type === "create_card" && ac.pipe)) {
      if (fromPhase) dsl += `  p${fromPhase} -. \"${safe(a.name)}\" .-> ep${act.pipe!.id}\n`;
    }
  }
  for (const a of makeAutos) {
    const fromPhase = a.trigger.phase?.id ?? structure.phases[0]?.id;
    if (fromPhase) dsl += `  p${fromPhase} -. \"${safe(a.name)}\" .-> make\n`;
  }

  // Styling
  dsl += `\n  classDef phase fill:#dbeafe,stroke:#3b82f6\n`;
  dsl += `  classDef ext fill:#dcfce7,stroke:#22c55e\n`;
  dsl += `  classDef db fill:#fef9c3,stroke:#eab308\n`;
  dsl += `  classDef make fill:#f3e8ff,stroke:#a855f7\n\n`;
  for (const ph of structure.phases) dsl += `  class p${ph.id} phase\n`;
  for (const id of seenExtPipes) dsl += `  class ep${id} ext\n`;
  for (const id of seenDbs) dsl += `  class db${id} db\n`;
  if (makeAutos.length > 0) dsl += `  class make make\n`;

  return {
    pipe_id,
    pipe_name: structure.name,
    mermaid_dsl: dsl,
    legend: {
      azul: "Fases do pipe",
      verde: "Pipes externos conectados",
      amarelo: "Databases",
      roxo: "Make.com",
      seta_solida: "Conexão via campo (connection/table field)",
      seta_pontilhada: "Automação (webhook Make ou cria card em pipe externo)",
    },
    usage_hint: "Cole mermaid_dsl em https://mermaid.live ou use mcp__claude_ai_Miro__diagram_create para criar no Miro.",
  };
}

// ── MCP Server ─────────────────────────────────────────────────────────────────
const server = new Server(
  { name: "pipefy-mcp", version: "2.0.0" },
  { capabilities: { tools: {} } }
);

server.setRequestHandler(ListToolsRequestSchema, async () => ({
  tools: [
    // ── Group 1: Investigation ──
    {
      name: "list_pipes",
      description: "Lista todos os pipes de uma organização Pipefy com fases e contagem de cards.",
      inputSchema: {
        type: "object",
        properties: {
          org_id: { type: "string", description: "ID numérico ou nome da organização (ex: 'Píer Geral' ou '301234'). Se omitido e houver apenas uma org, usa ela automaticamente." },
        },
        required: [],
      },
    },
    {
      name: "get_pipe_structure",
      description: "Retorna estrutura completa de um pipe: fases, campos, webhooks, membros e conexões.",
      inputSchema: {
        type: "object",
        properties: {
          pipe_id: { type: "string", description: "ID do pipe" },
        },
        required: ["pipe_id"],
      },
    },
    {
      name: "get_pipe_automations",
      description: "Lista automações de um pipe com classificação (interno, webhook, cria_card_externo, email).",
      inputSchema: {
        type: "object",
        properties: {
          pipe_id: { type: "string", description: "ID do pipe" },
        },
        required: ["pipe_id"],
      },
    },
    {
      name: "list_databases",
      description: "Lista todos os databases (tabelas) de uma organização com campos e membros.",
      inputSchema: {
        type: "object",
        properties: {
          org_id: { type: "string", description: "ID numérico ou nome da organização (ex: 'Píer Geral' ou '301234'). Opcional." },
        },
        required: [],
      },
    },
    {
      name: "get_database_structure",
      description: "Retorna estrutura completa de um database com campos e 3 registros de exemplo.",
      inputSchema: {
        type: "object",
        properties: {
          table_id: { type: "string", description: "ID do database/tabela" },
        },
        required: ["table_id"],
      },
    },
    {
      name: "trace_card_journey",
      description: "Rastreia a jornada completa de um card: campos, comentários, cards relacionados.",
      inputSchema: {
        type: "object",
        properties: {
          card_id: { type: "string", description: "ID do card" },
        },
        required: ["card_id"],
      },
    },
    // ── Group 2: Connection Mapping ──
    {
      name: "map_full_connections",
      description: "Mapeia todas as conexões entre pipes e databases de uma organização, incluindo webhooks Make.",
      inputSchema: {
        type: "object",
        properties: {
          org_id: { type: "string", description: "ID numérico ou nome da organização (ex: 'Píer Geral' ou '301234'). Opcional." },
        },
        required: [],
      },
    },
    {
      name: "map_pipe_dependencies",
      description: "Mapeia dependências de um pipe até 2 níveis de profundidade (pipes e databases conectados).",
      inputSchema: {
        type: "object",
        properties: {
          pipe_id: { type: "string", description: "ID do pipe" },
        },
        required: ["pipe_id"],
      },
    },
    {
      name: "analyze_field_dependencies",
      description: "Analisa dependências entre campos de um pipe (quais campos controlam outros via conditions).",
      inputSchema: {
        type: "object",
        properties: {
          pipe_id: { type: "string", description: "ID do pipe" },
        },
        required: ["pipe_id"],
      },
    },
    // ── Group 3: Make Bridge ──
    {
      name: "generate_make_query",
      description: "Gera query/mutation GraphQL pronta para usar no Make.com, com configuração HTTP e mapeamento de outputs.",
      inputSchema: {
        type: "object",
        properties: {
          pipe_id: { type: "string", description: "ID do pipe" },
          operation: {
            type: "string",
            enum: [
              "get_card",
              "list_cards",
              "create_card",
              "move_card",
              "update_field",
              "get_database_record",
              "create_database_record",
            ],
            description: "Tipo de operação",
          },
          options: { type: "object", description: "Opções adicionais (opcional)" },
        },
        required: ["pipe_id", "operation"],
      },
    },
    {
      name: "generate_webhook_payload_schema",
      description: "Gera o schema do payload de um evento Pipefy e as variáveis Make correspondentes.",
      inputSchema: {
        type: "object",
        properties: {
          pipe_id: { type: "string", description: "ID do pipe" },
          trigger_type: {
            type: "string",
            description: "Tipo de trigger (card.create, card.move, card.done, card.expired, card.late, card.field_update)",
          },
        },
        required: ["pipe_id", "trigger_type"],
      },
    },
    // ── Group 4: Construction ──
    {
      name: "create_pipe",
      description: "Cria um novo pipe em uma organização Pipefy.",
      inputSchema: {
        type: "object",
        properties: {
          org_id: { type: "string", description: "ID numérico ou nome da organização (ex: 'Píer Geral' ou '301234')" },
          name: { type: "string", description: "Nome do pipe" },
          description: { type: "string", description: "Descrição (opcional)" },
        },
        required: ["org_id", "name"],
      },
    },
    {
      name: "create_phase",
      description: "Cria uma nova fase em um pipe.",
      inputSchema: {
        type: "object",
        properties: {
          pipe_id: { type: "string", description: "ID do pipe" },
          name: { type: "string", description: "Nome da fase" },
          description: { type: "string", description: "Descrição (opcional)" },
        },
        required: ["pipe_id", "name"],
      },
    },
    {
      name: "create_field",
      description: "Cria um campo em uma fase de pipe.",
      inputSchema: {
        type: "object",
        properties: {
          phase_id: { type: "string", description: "ID da fase" },
          label: { type: "string", description: "Rótulo do campo" },
          type: { type: "string", description: "Tipo do campo (short_text, number, select, connection, etc.)" },
          required: { type: "boolean", description: "Campo obrigatório (opcional)" },
          options: { type: "array", items: { type: "string" }, description: "Opções para select/radio/checkbox" },
          connected_pipe_id: { type: "string", description: "ID do pipe conectado (para type=connection)" },
          connected_table_id: { type: "string", description: "ID do database conectado (para type=table)" },
        },
        required: ["phase_id", "label", "type"],
      },
    },
    {
      name: "create_start_form_field",
      description: "Cria um campo no formulário de abertura de um pipe.",
      inputSchema: {
        type: "object",
        properties: {
          pipe_id: { type: "string", description: "ID do pipe" },
          label: { type: "string", description: "Rótulo do campo" },
          type: { type: "string", description: "Tipo do campo" },
          required: { type: "boolean", description: "Obrigatório (opcional)" },
          options: { type: "array", items: { type: "string" }, description: "Opções para select/radio/checkbox" },
        },
        required: ["pipe_id", "label", "type"],
      },
    },
    {
      name: "update_phase",
      description: "Atualiza nome ou descrição de uma fase.",
      inputSchema: {
        type: "object",
        properties: {
          phase_id: { type: "string", description: "ID da fase" },
          updates: {
            type: "object",
            description: "Campos a atualizar: { name?, description? }",
          },
        },
        required: ["phase_id", "updates"],
      },
    },
    {
      name: "update_field",
      description: "Atualiza propriedades de um campo (label, required, options).",
      inputSchema: {
        type: "object",
        properties: {
          field_id: { type: "string", description: "ID do campo" },
          updates: {
            type: "object",
            description: "Campos a atualizar: { label?, required?, options? }",
          },
        },
        required: ["field_id", "updates"],
      },
    },
    // ── Group 5: Automations ──
    {
      name: "get_valid_automation_types",
      description: "Retorna os tipos válidos de triggers, operadores e actions para criar automações no Pipefy.",
      inputSchema: {
        type: "object",
        properties: {},
        required: [],
      },
    },
    {
      name: "create_automation",
      description: "Cria uma automação em um pipe com trigger, conditions e actions.",
      inputSchema: {
        type: "object",
        properties: {
          pipe_id: { type: "string", description: "ID do pipe" },
          name: { type: "string", description: "Nome da automação" },
          trigger: { type: "object", description: "Objeto trigger com type e opcionalmente phase/field" },
          conditions: { type: "array", description: "Array de conditions (opcional)" },
          actions: { type: "array", description: "Array de actions" },
        },
        required: ["pipe_id", "name", "trigger", "actions"],
      },
    },
    {
      name: "get_automation_detail",
      description: "Retorna detalhes humanizados de uma automação específica de um pipe.",
      inputSchema: {
        type: "object",
        properties: {
          automation_id: { type: "string", description: "ID da automação" },
          pipe_id: { type: "string", description: "ID do pipe que contém a automação" },
        },
        required: ["automation_id", "pipe_id"],
      },
    },
    // ── Group 6: Dynamic Exploration ──
    {
      name: "introspect_schema",
      description: "Executa introspection no schema GraphQL do Pipefy. Usa cache de 30 minutos.",
      inputSchema: {
        type: "object",
        properties: {
          type_name: { type: "string", description: "Filtrar por nome de tipo (opcional)" },
        },
        required: [],
      },
    },
    {
      name: "build_and_run_query",
      description: "Retorna contexto de schema e pipe para o Claude construir uma query GraphQL customizada.",
      inputSchema: {
        type: "object",
        properties: {
          intent: { type: "string", description: "Descrição do que você quer fazer" },
          variables: { type: "object", description: "Variáveis de hint (opcional)" },
          pipe_id: { type: "string", description: "ID do pipe para contexto adicional (opcional)" },
        },
        required: ["intent"],
      },
    },
    {
      name: "call_pipefy_api",
      description: "Executa qualquer query ou mutation GraphQL diretamente na API do Pipefy.",
      inputSchema: {
        type: "object",
        properties: {
          query: { type: "string", description: "Query ou mutation GraphQL" },
          variables: { type: "object", description: "Variáveis da query (opcional)" },
        },
        required: ["query"],
      },
    },
    // ── Group 12: Health & Diagram ──
    {
      name: "analyze_pipe_health",
      description: "Analisa a saúde de um pipe e retorna um score + lista de problemas detectados: automações inativas, integrações Make quebradas, automações conflitantes, gargalos de fase, campos sem uso e cards sem responsável.",
      inputSchema: {
        type: "object",
        properties: {
          pipe_id: { type: "string", description: "ID do pipe" },
        },
        required: ["pipe_id"],
      },
    },
    {
      name: "export_pipe_as_diagram_dsl",
      description: "Gera o fluxo do pipe em DSL Mermaid (flowchart): fases, conexões pipe→pipe, pipe→database, automações webhook para Make e automações que criam cards em outros pipes. Cole em mermaid.live ou use com o Miro MCP.",
      inputSchema: {
        type: "object",
        properties: {
          pipe_id: { type: "string", description: "ID do pipe" },
        },
        required: ["pipe_id"],
      },
    },
    // ── Group 10: Metrics ──
    {
      name: "get_pipe_metrics",
      description: "Retorna métricas de uso de um pipe: distribuição de cards por fase, gargalo atual, taxa de conclusão, volume por dia e top assignees. Base para análise de uso e sugestão de melhorias.",
      inputSchema: {
        type: "object",
        properties: {
          pipe_id: { type: "string", description: "ID do pipe" },
        },
        required: ["pipe_id"],
      },
    },
    // ── Group 11: Integration Map ──
    {
      name: "map_pipefy_make_integration",
      description: "Documenta a integração bidirecional Pipefy↔Make de um pipe: automações que disparam webhooks Make, webhooks de entrada, connections pipe/database, schemas de payload por trigger e orientações para cruzar com Make-MCP.",
      inputSchema: {
        type: "object",
        properties: {
          pipe_id: { type: "string", description: "ID do pipe" },
        },
        required: ["pipe_id"],
      },
    },
    // ── Group 7: Cards ──
    {
      name: "list_cards",
      description: "Lista cards de um pipe com filtros opcionais por fase e busca por título. Retorna campos, fase atual e assignees de cada card.",
      inputSchema: {
        type: "object",
        properties: {
          pipe_id: { type: "string", description: "ID do pipe" },
          phase_id: { type: "string", description: "Filtrar por fase específica (opcional)" },
          limit: { type: "number", description: "Quantidade máxima de cards a retornar (default 30)" },
          search: { type: "string", description: "Filtrar por texto no título do card (opcional)" },
        },
        required: ["pipe_id"],
      },
    },
    // ── Group 8: Webhook Logs ──
    {
      name: "get_webhook_dispatch_logs",
      description: "Retorna configurações de webhooks e automações que disparam webhooks em um pipe, incluindo URLs Make.com identificadas. Ponto de partida para diagnosticar falhas na integração Pipefy→Make.",
      inputSchema: {
        type: "object",
        properties: {
          pipe_id: { type: "string", description: "ID do pipe" },
        },
        required: ["pipe_id"],
      },
    },
    // ── Group 9: Documentation ──
    {
      name: "document_flow",
      description: "Gera documentação completa em markdown de um pipe: fases, campos, conexões com outros pipes/databases, automações em linguagem natural e integrações Make.com. Use para documentar fluxos complexos.",
      inputSchema: {
        type: "object",
        properties: {
          pipe_id: { type: "string", description: "ID do pipe a documentar" },
        },
        required: ["pipe_id"],
      },
    },
  ],
}));

server.setRequestHandler(CallToolRequestSchema, async (request) => {
  const { name, arguments: args } = request.params;

  try {
    let result: unknown;

    switch (name) {
      case "list_pipes":
        result = await listPipes(args?.org_id as string | undefined);
        break;

      case "get_pipe_structure":
        result = await getPipeStructure(args?.pipe_id as string);
        break;

      case "get_pipe_automations":
        result = await getPipeAutomations(args?.pipe_id as string);
        break;

      case "list_databases":
        result = await listDatabases(args?.org_id as string | undefined);
        break;

      case "get_database_structure":
        result = await getDatabaseStructure(args?.table_id as string);
        break;

      case "trace_card_journey":
        result = await traceCardJourney(args?.card_id as string);
        break;

      case "map_full_connections":
        result = await mapFullConnections(args?.org_id as string | undefined);
        break;

      case "map_pipe_dependencies":
        result = await mapPipeDependencies(args?.pipe_id as string);
        break;

      case "analyze_field_dependencies":
        result = await analyzeFieldDependencies(args?.pipe_id as string);
        break;

      case "generate_make_query":
        result = await generateMakeQuery(
          args?.pipe_id as string,
          args?.operation as MakeOperation,
          args?.options as object | undefined
        );
        break;

      case "generate_webhook_payload_schema":
        result = await generateWebhookPayloadSchema(
          args?.pipe_id as string,
          args?.trigger_type as string
        );
        break;

      case "create_pipe":
        result = await createPipe(
          args?.org_id as string,
          args?.name as string,
          args?.description as string | undefined
        );
        break;

      case "create_phase":
        result = await createPhase(
          args?.pipe_id as string,
          args?.name as string,
          args?.description as string | undefined
        );
        break;

      case "create_field":
        result = await createField(
          args?.phase_id as string,
          args?.label as string,
          args?.type as string,
          args?.required as boolean | undefined,
          args?.options as string[] | undefined,
          args?.connected_pipe_id as string | undefined,
          args?.connected_table_id as string | undefined
        );
        break;

      case "create_start_form_field":
        result = await createStartFormField(
          args?.pipe_id as string,
          args?.label as string,
          args?.type as string,
          args?.required as boolean | undefined,
          args?.options as string[] | undefined
        );
        break;

      case "update_phase":
        result = await updatePhase(
          args?.phase_id as string,
          args?.updates as { name?: string; description?: string }
        );
        break;

      case "update_field":
        result = await updateField(
          args?.field_id as string,
          args?.updates as { label?: string; required?: boolean; options?: string[] }
        );
        break;

      case "get_valid_automation_types":
        result = await getValidAutomationTypes();
        break;

      case "create_automation":
        result = await createAutomation(
          args?.pipe_id as string,
          args?.name as string,
          args?.trigger as object,
          args?.conditions as object[] | undefined,
          args?.actions as object[]
        );
        break;

      case "get_automation_detail":
        result = await getAutomationDetail(
          args?.automation_id as string,
          args?.pipe_id as string
        );
        break;

      case "introspect_schema":
        result = await introspectSchema(args?.type_name as string | undefined);
        break;

      case "build_and_run_query":
        result = await buildAndRunQuery(
          args?.intent as string,
          args?.variables as object | undefined,
          args?.pipe_id as string | undefined
        );
        break;

      case "call_pipefy_api":
        result = await callPipefyApi(
          args?.query as string,
          args?.variables as object | undefined
        );
        break;

      case "analyze_pipe_health":
        result = await analyzePipeHealth(args?.pipe_id as string);
        break;

      case "export_pipe_as_diagram_dsl":
        result = await exportPipeAsDiagramDsl(args?.pipe_id as string);
        break;

      case "get_pipe_metrics":
        result = await getPipeMetrics(args?.pipe_id as string);
        break;

      case "map_pipefy_make_integration":
        result = await mapPipefyMakeIntegration(args?.pipe_id as string);
        break;

      case "list_cards":
        result = await listCards(
          args?.pipe_id as string,
          args?.phase_id as string | undefined,
          args?.limit as number | undefined,
          args?.search as string | undefined
        );
        break;

      case "get_webhook_dispatch_logs":
        result = await getWebhookDispatchLogs(args?.pipe_id as string);
        break;

      case "document_flow":
        result = await documentFlow(args?.pipe_id as string);
        break;

      default:
        throw new Error(`Ferramenta desconhecida: ${name}`);
    }

    return {
      content: [{ type: "text", text: JSON.stringify(result, null, 2) }],
    };
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return {
      content: [{ type: "text", text: `Erro em ${name}: ${message}` }],
      isError: true,
    };
  }
});

// ── Start ──────────────────────────────────────────────────────────────────────
const PORT = process.env.PORT ? parseInt(process.env.PORT) : undefined;

if (PORT) {
  // HTTP/SSE mode — usado no Railway / LibreChat remoto
  let sseTransport: SSEServerTransport | null = null;

  const httpServer = http.createServer(async (req, res) => {
    res.setHeader("Access-Control-Allow-Origin", "*");
    res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
    res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");

    if (req.method === "OPTIONS") {
      res.writeHead(204);
      res.end();
      return;
    }

    // Bearer token authentication (skipped for /health)
    if (req.url !== "/health") {
      const accessToken = process.env.MCP_ACCESS_TOKEN;
      if (accessToken) {
        const authHeader = req.headers["authorization"] ?? "";
        const token = authHeader.startsWith("Bearer ") ? authHeader.slice(7) : "";
        if (token !== accessToken) {
          res.writeHead(401, { "Content-Type": "text/plain" });
          res.end("Unauthorized");
          return;
        }
      }
    }

    if (req.method === "GET" && req.url === "/sse") {
      if (sseTransport) {
        try { await server.close(); } catch { /* ignore */ }
      }
      sseTransport = new SSEServerTransport("/message", res);
      await server.connect(sseTransport);
    } else if (req.method === "POST" && req.url?.startsWith("/message")) {
      if (sseTransport) {
        await sseTransport.handlePostMessage(req, res);
      } else {
        res.writeHead(503, { "Content-Type": "text/plain" });
        res.end("No active SSE session");
      }
    } else if (req.url === "/health") {
      res.writeHead(200, { "Content-Type": "text/plain" });
      res.end("OK");
    } else {
      res.writeHead(404);
      res.end();
    }
  });

  httpServer.listen(PORT, () => {
    process.stderr.write(`[pipefy-mcp] SSE server listening on port ${PORT}\n`);
  });
} else {
  // stdio mode — usado localmente (Claude Code, etc.)
  const transport = new StdioServerTransport();
  await server.connect(transport);
}
