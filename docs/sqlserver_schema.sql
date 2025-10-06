-- Esquema SQL Server para la base de Branch History de ForgeBuild (sin datos)
-- Este script crea todas las tablas, claves y índices requeridos.

CREATE TABLE dbo.branches (
    [key] NVARCHAR(255) NOT NULL PRIMARY KEY,
    branch NVARCHAR(255) NOT NULL,
    group_name NVARCHAR(255) NULL,
    project NVARCHAR(255) NULL,
    created_at BIGINT NOT NULL DEFAULT 0,
    created_by NVARCHAR(255) NULL,
    exists_local BIT NOT NULL DEFAULT 0,
    exists_origin BIT NOT NULL DEFAULT 0,
    merge_status NVARCHAR(64) NULL,
    diverged BIT NULL,
    stale_days INT NULL,
    last_action NVARCHAR(64) NULL,
    last_updated_at BIGINT NOT NULL DEFAULT 0,
    last_updated_by NVARCHAR(255) NULL
);

CREATE TABLE dbo.activity_log (
    id INT IDENTITY(1,1) PRIMARY KEY,
    ts BIGINT NOT NULL,
    [user] NVARCHAR(255) NULL,
    group_name NVARCHAR(255) NULL,
    project NVARCHAR(255) NULL,
    branch NVARCHAR(255) NULL,
    action NVARCHAR(64) NULL,
    result NVARCHAR(64) NULL,
    message NVARCHAR(1024) NULL,
    branch_key NVARCHAR(512) NULL,
    CONSTRAINT uq_activity UNIQUE (ts, [user], group_name, project, branch, action, result, message)
);

CREATE TABLE dbo.sprints (
    id INT IDENTITY(1,1) PRIMARY KEY,
    branch_key NVARCHAR(512) NOT NULL DEFAULT '',
    qa_branch_key NVARCHAR(512) NULL,
    name NVARCHAR(255) NOT NULL DEFAULT '',
    version NVARCHAR(128) NOT NULL DEFAULT '',
    lead_user NVARCHAR(255) NULL,
    qa_user NVARCHAR(255) NULL,
    company_id INT NULL,
    company_sequence INT NULL,
    description NVARCHAR(MAX) NULL,
    status NVARCHAR(32) NOT NULL DEFAULT 'open',
    closed_at BIGINT NULL,
    closed_by NVARCHAR(255) NULL,
    created_at BIGINT NOT NULL DEFAULT 0,
    created_by NVARCHAR(255) NULL,
    updated_at BIGINT NOT NULL DEFAULT 0,
    updated_by NVARCHAR(255) NULL
);

CREATE TABLE dbo.sprint_groups (
    sprint_id INT NOT NULL PRIMARY KEY,
    group_name NVARCHAR(255) NOT NULL,
    CONSTRAINT fk_sprint_groups_sprint FOREIGN KEY (sprint_id) REFERENCES dbo.sprints(id) ON DELETE CASCADE
);

CREATE TABLE dbo.cards (
    id INT IDENTITY(1,1) PRIMARY KEY,
    sprint_id INT NULL,
    branch_key NVARCHAR(512) NULL,
    title NVARCHAR(255) NOT NULL DEFAULT '',
    ticket_id NVARCHAR(128) NULL,
    branch NVARCHAR(255) NOT NULL DEFAULT '',
    group_name NVARCHAR(255) NULL,
    assignee NVARCHAR(255) NULL,
    qa_assignee NVARCHAR(255) NULL,
    description NVARCHAR(MAX) NULL,
    unit_tests_url NVARCHAR(1024) NULL,
    qa_url NVARCHAR(1024) NULL,
    unit_tests_done BIT NOT NULL DEFAULT 0,
    qa_done BIT NOT NULL DEFAULT 0,
    unit_tests_by NVARCHAR(255) NULL,
    qa_by NVARCHAR(255) NULL,
    unit_tests_at BIGINT NULL,
    qa_at BIGINT NULL,
    status NVARCHAR(32) NOT NULL DEFAULT 'pending',
    company_id INT NULL,
    incidence_type_id INT NULL,
    closed_at BIGINT NULL,
    closed_by NVARCHAR(255) NULL,
    branch_created_by NVARCHAR(255) NULL,
    branch_created_at BIGINT NULL,
    branch_created_flag BIT NOT NULL DEFAULT 0,
    created_at BIGINT NOT NULL DEFAULT 0,
    created_by NVARCHAR(255) NULL,
    updated_at BIGINT NOT NULL DEFAULT 0,
    updated_by NVARCHAR(255) NULL,
    CONSTRAINT fk_cards_sprint FOREIGN KEY (sprint_id) REFERENCES dbo.sprints(id) ON DELETE SET NULL
);

CREATE TABLE dbo.catalog_incidence_types (
    id INT IDENTITY(1,1) PRIMARY KEY,
    name NVARCHAR(255) NOT NULL UNIQUE,
    icon VARBINARY(MAX) NULL,
    created_at BIGINT NOT NULL DEFAULT 0,
    created_by NVARCHAR(255) NULL,
    updated_at BIGINT NOT NULL DEFAULT 0,
    updated_by NVARCHAR(255) NULL
);

CREATE TABLE dbo.card_sprint_links (
    id INT IDENTITY(1,1) PRIMARY KEY,
    card_id INT NOT NULL,
    sprint_id INT NULL,
    assigned_at BIGINT NOT NULL DEFAULT 0,
    assigned_by NVARCHAR(255) NULL,
    unassigned_at BIGINT NULL,
    unassigned_by NVARCHAR(255) NULL,
    CONSTRAINT fk_card_sprint_card FOREIGN KEY (card_id) REFERENCES dbo.cards(id) ON DELETE CASCADE,
    CONSTRAINT fk_card_sprint_sprint FOREIGN KEY (sprint_id) REFERENCES dbo.sprints(id)
);

CREATE TABLE dbo.card_scripts (
    id INT IDENTITY(1,1) PRIMARY KEY,
    card_id INT NOT NULL UNIQUE,
    file_name NVARCHAR(255) NULL,
    content NVARCHAR(MAX) NOT NULL,
    created_at BIGINT NOT NULL DEFAULT 0,
    created_by NVARCHAR(255) NULL,
    updated_at BIGINT NOT NULL DEFAULT 0,
    updated_by NVARCHAR(255) NULL,
    CONSTRAINT fk_card_scripts_card FOREIGN KEY (card_id) REFERENCES dbo.cards(id) ON DELETE CASCADE
);

CREATE TABLE dbo.catalog_companies (
    id INT IDENTITY(1,1) PRIMARY KEY,
    name NVARCHAR(255) NOT NULL UNIQUE,
    group_name NVARCHAR(255) NULL,
    next_sprint_number INT NOT NULL DEFAULT 1,
    created_at BIGINT NOT NULL DEFAULT 0,
    created_by NVARCHAR(255) NULL,
    updated_at BIGINT NOT NULL DEFAULT 0,
    updated_by NVARCHAR(255) NULL
);

CREATE TABLE dbo.card_company_links (
    id INT IDENTITY(1,1) PRIMARY KEY,
    card_id INT NOT NULL,
    company_id INT NOT NULL,
    linked_at BIGINT NOT NULL DEFAULT 0,
    linked_by NVARCHAR(255) NULL,
    unlinked_at BIGINT NULL,
    unlinked_by NVARCHAR(255) NULL,
    CONSTRAINT fk_card_company_card FOREIGN KEY (card_id) REFERENCES dbo.cards(id) ON DELETE CASCADE,
    CONSTRAINT fk_card_company_company FOREIGN KEY (company_id) REFERENCES dbo.catalog_companies(id) ON DELETE CASCADE
);

CREATE TABLE dbo.card_branch_links (
    id INT IDENTITY(1,1) PRIMARY KEY,
    card_id INT NOT NULL,
    branch_key NVARCHAR(512) NOT NULL,
    linked_at BIGINT NOT NULL DEFAULT 0,
    linked_by NVARCHAR(255) NULL,
    unlinked_at BIGINT NULL,
    unlinked_by NVARCHAR(255) NULL,
    CONSTRAINT fk_card_branch_card FOREIGN KEY (card_id) REFERENCES dbo.cards(id) ON DELETE CASCADE
);

CREATE TABLE dbo.users (
    username NVARCHAR(255) NOT NULL PRIMARY KEY,
    display_name NVARCHAR(255) NOT NULL,
    email NVARCHAR(255) NULL,
    active BIT NOT NULL DEFAULT 1,
    password_hash NVARCHAR(512) NULL,
    password_salt NVARCHAR(512) NULL,
    password_algo NVARCHAR(128) NULL,
    password_changed_at BIGINT NULL,
    require_password_reset BIT NOT NULL CONSTRAINT DF_users_require_password_reset DEFAULT (0),
    active_since BIGINT NULL
);

CREATE TABLE dbo.roles (
    [key] NVARCHAR(128) NOT NULL PRIMARY KEY,
    name NVARCHAR(255) NOT NULL,
    description NVARCHAR(512) NULL
);

CREATE TABLE dbo.user_roles (
    id INT IDENTITY(1,1) PRIMARY KEY,
    username NVARCHAR(255) NOT NULL,
    role_key NVARCHAR(128) NOT NULL,
    CONSTRAINT uq_user_roles UNIQUE (username, role_key),
    CONSTRAINT fk_user_roles_user FOREIGN KEY (username) REFERENCES dbo.users(username) ON DELETE CASCADE,
    CONSTRAINT fk_user_roles_role FOREIGN KEY (role_key) REFERENCES dbo.roles([key]) ON DELETE CASCADE
);

CREATE TABLE dbo.branch_local_users (
    branch_key NVARCHAR(255) NOT NULL,
    username NVARCHAR(255) NOT NULL,
    state NVARCHAR(32) NOT NULL DEFAULT 'absent',
    location NVARCHAR(1024) NULL,
    updated_at BIGINT NOT NULL DEFAULT 0,
    CONSTRAINT pk_branch_local_users PRIMARY KEY (branch_key, username),
    CONSTRAINT fk_branch_local_users_branch FOREIGN KEY (branch_key) REFERENCES dbo.branches([key]) ON DELETE CASCADE
);

CREATE INDEX idx_activity_branch_key ON dbo.activity_log (branch_key);
CREATE INDEX idx_activity_ts ON dbo.activity_log (ts DESC, id DESC);
CREATE INDEX idx_sprints_branch ON dbo.sprints (branch_key);
CREATE INDEX idx_cards_sprint ON dbo.cards (sprint_id);
CREATE INDEX idx_cards_branch ON dbo.cards (branch);
CREATE INDEX idx_card_sprint_active ON dbo.card_sprint_links (card_id, unassigned_at);
CREATE INDEX idx_card_sprint_by_sprint ON dbo.card_sprint_links (sprint_id, unassigned_at);
CREATE INDEX idx_card_company_active ON dbo.card_company_links (card_id, unlinked_at);
CREATE INDEX idx_card_branch_active ON dbo.card_branch_links (card_id, unlinked_at);
CREATE INDEX idx_branch_local_users_username ON dbo.branch_local_users (username);
CREATE INDEX idx_branch_local_users_state ON dbo.branch_local_users (state);

-- Tablas de configuración compartida (prefijo config_)

CREATE TABLE dbo.config_metadata (
    [key] NVARCHAR(255) NOT NULL PRIMARY KEY,
    value NVARCHAR(MAX) NOT NULL,
    created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);

CREATE TABLE dbo.config_groups (
    [key] NVARCHAR(255) NOT NULL PRIMARY KEY,
    position INT NOT NULL,
    output_base NVARCHAR(MAX) NOT NULL DEFAULT '',
    config_json NVARCHAR(MAX) NOT NULL DEFAULT '{}',
    created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);

CREATE TRIGGER dbo.trg_config_groups_updated
ON dbo.config_groups
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE dbo.config_groups
    SET updated_at = SYSUTCDATETIME()
    WHERE [key] IN (SELECT DISTINCT [key] FROM Inserted);
END;
GO

CREATE TABLE dbo.config_group_repos (
    id INT IDENTITY(1,1) PRIMARY KEY,
    group_key NVARCHAR(255) NOT NULL,
    repo_key NVARCHAR(255) NOT NULL,
    path NVARCHAR(MAX) NOT NULL,
    CONSTRAINT fk_config_group_repos_group FOREIGN KEY (group_key) REFERENCES dbo.config_groups([key]) ON DELETE CASCADE
);

CREATE TABLE dbo.config_group_profiles (
    id INT IDENTITY(1,1) PRIMARY KEY,
    group_key NVARCHAR(255) NOT NULL,
    position INT NOT NULL,
    profile NVARCHAR(255) NOT NULL,
    CONSTRAINT fk_config_group_profiles_group FOREIGN KEY (group_key) REFERENCES dbo.config_groups([key]) ON DELETE CASCADE
);

CREATE TABLE dbo.config_group_user_paths (
    id INT IDENTITY(1,1) PRIMARY KEY,
    group_key NVARCHAR(255) NOT NULL,
    username NVARCHAR(255) NOT NULL,
    kind NVARCHAR(64) NOT NULL,
    item_key NVARCHAR(255) NOT NULL DEFAULT '',
    value NVARCHAR(MAX) NOT NULL,
    CONSTRAINT uq_config_group_user_paths UNIQUE (group_key, username, kind, item_key),
    CONSTRAINT fk_config_group_user_paths_group FOREIGN KEY (group_key) REFERENCES dbo.config_groups([key]) ON DELETE CASCADE
);

CREATE TABLE dbo.config_projects (
    id INT IDENTITY(1,1) PRIMARY KEY,
    group_key NVARCHAR(255) NOT NULL,
    project_key NVARCHAR(255) NOT NULL,
    position INT NOT NULL,
    execution_mode NVARCHAR(64) NULL,
    workspace NVARCHAR(255) NULL,
    repo NVARCHAR(255) NULL,
    config_json NVARCHAR(MAX) NOT NULL DEFAULT '{}',
    CONSTRAINT fk_config_projects_group FOREIGN KEY (group_key) REFERENCES dbo.config_groups([key]) ON DELETE CASCADE
);

CREATE UNIQUE INDEX idx_config_projects_group_key ON dbo.config_projects (group_key, project_key);

CREATE TABLE dbo.config_project_modules (
    id INT IDENTITY(1,1) PRIMARY KEY,
    project_id INT NOT NULL,
    position INT NOT NULL,
    name NVARCHAR(255) NOT NULL,
    path NVARCHAR(MAX) NOT NULL,
    version_files NVARCHAR(MAX) NOT NULL DEFAULT '[]',
    goals NVARCHAR(MAX) NOT NULL DEFAULT '[]',
    optional BIT NOT NULL DEFAULT 0,
    profile_override NVARCHAR(255) NULL,
    only_if_profile_equals NVARCHAR(255) NULL,
    copy_to_profile_war BIT NOT NULL DEFAULT 0,
    copy_to_profile_ui BIT NOT NULL DEFAULT 0,
    copy_to_subfolder NVARCHAR(255) NULL,
    rename_jar_to NVARCHAR(255) NULL,
    no_profile BIT NOT NULL DEFAULT 0,
    run_once BIT NOT NULL DEFAULT 0,
    select_pattern NVARCHAR(255) NULL,
    serial_across_profiles BIT NOT NULL DEFAULT 0,
    copy_to_root BIT NOT NULL DEFAULT 0,
    config_json NVARCHAR(MAX) NOT NULL DEFAULT '{}',
    CONSTRAINT fk_config_project_modules_project FOREIGN KEY (project_id) REFERENCES dbo.config_projects(id) ON DELETE CASCADE
);

CREATE TABLE dbo.config_module_user_paths (
    id INT IDENTITY(1,1) PRIMARY KEY,
    group_key NVARCHAR(255) NOT NULL,
    project_key NVARCHAR(255) NOT NULL,
    module_name NVARCHAR(255) NOT NULL,
    username NVARCHAR(255) NOT NULL,
    path NVARCHAR(MAX) NOT NULL,
    CONSTRAINT uq_config_module_user_paths UNIQUE (group_key, project_key, module_name, username),
    CONSTRAINT fk_config_module_user_paths_group FOREIGN KEY (group_key) REFERENCES dbo.config_groups([key]) ON DELETE CASCADE
);

CREATE TABLE dbo.config_deploy_targets (
    id INT IDENTITY(1,1) PRIMARY KEY,
    group_key NVARCHAR(255) NOT NULL,
    position INT NOT NULL,
    name NVARCHAR(255) NOT NULL,
    project_key NVARCHAR(255) NOT NULL,
    path_template NVARCHAR(MAX) NOT NULL,
    hotfix_path_template NVARCHAR(MAX) NULL,
    config_json NVARCHAR(MAX) NOT NULL DEFAULT '{}',
    CONSTRAINT fk_config_deploy_targets_group FOREIGN KEY (group_key) REFERENCES dbo.config_groups([key]) ON DELETE CASCADE
);

CREATE TABLE dbo.config_deploy_target_profiles (
    id INT IDENTITY(1,1) PRIMARY KEY,
    target_id INT NOT NULL,
    position INT NOT NULL,
    profile NVARCHAR(255) NOT NULL,
    CONSTRAINT fk_config_deploy_target_profiles_target FOREIGN KEY (target_id) REFERENCES dbo.config_deploy_targets(id) ON DELETE CASCADE
);

CREATE TABLE dbo.config_deploy_user_paths (
    id INT IDENTITY(1,1) PRIMARY KEY,
    group_key NVARCHAR(255) NOT NULL,
    target_name NVARCHAR(255) NOT NULL,
    username NVARCHAR(255) NOT NULL,
    path_template NVARCHAR(MAX) NULL,
    hotfix_path_template NVARCHAR(MAX) NULL,
    CONSTRAINT uq_config_deploy_user_paths UNIQUE (group_key, target_name, username),
    CONSTRAINT fk_config_deploy_user_paths_group FOREIGN KEY (group_key) REFERENCES dbo.config_groups([key]) ON DELETE CASCADE
);
