use std::path::Path;

use crate::{
    GitTaskWorktreeProvisioner, ScheduledTaskTargetValidator, TaskWorktreeProvisioner,
    TaskWorktreeProvisionerError,
};

/// Uses the same Git capability probe as interactive task worktree creation.
#[derive(Debug, Clone, Copy, Default)]
pub struct GitScheduledTaskTargetValidator;

impl ScheduledTaskTargetValidator for GitScheduledTaskTargetValidator {
    fn validate_git_worktree_target(
        &self,
        project_root: &Path,
    ) -> Result<(), TaskWorktreeProvisionerError> {
        if !project_root.is_dir() {
            return Err(TaskWorktreeProvisionerError::NotARepository);
        }
        GitTaskWorktreeProvisioner::new(project_root.to_path_buf()).validate_repository()
    }

    fn resolve_default_branch(
        &self,
        project_root: &Path,
    ) -> Result<Option<String>, TaskWorktreeProvisionerError> {
        if !project_root.is_dir() {
            return Err(TaskWorktreeProvisionerError::NotARepository);
        }
        GitTaskWorktreeProvisioner::new(project_root.to_path_buf()).resolve_default_branch()
    }
}

#[cfg(test)]
mod tests {
    use std::process::Command;

    use super::*;

    #[test]
    fn rejects_a_missing_or_non_git_project_directory() {
        let fixture = tempfile::tempdir().expect("create fixture");
        let missing = fixture.path().join("missing");

        assert!(matches!(
            GitScheduledTaskTargetValidator.validate_git_worktree_target(&missing),
            Err(TaskWorktreeProvisionerError::NotARepository)
        ));
        assert!(matches!(
            GitScheduledTaskTargetValidator.validate_git_worktree_target(fixture.path()),
            Err(TaskWorktreeProvisionerError::NotARepository)
        ));
    }

    #[test]
    fn accepts_an_existing_git_project_directory_without_mutating_it() {
        let fixture = tempfile::tempdir().expect("create fixture");
        let initialized = Command::new("git")
            .arg("init")
            .arg("--quiet")
            .arg(fixture.path())
            .status()
            .expect("run git init");
        assert!(initialized.success());

        GitScheduledTaskTargetValidator
            .validate_git_worktree_target(fixture.path())
            .expect("validate git target");
    }
}
