//! Publishes and withdraws native backend readiness.
use super::{
    BackendSchemaEvidence, CollaborationRuntime, NonEmptyText, ObservationTimestamp, SchemaDigest,
    io,
};
use collaboration_protocol::CodexGeneration;

impl CollaborationRuntime {
    pub async fn backend_ready(
        &mut self,
        at: ObservationTimestamp,
        schema: Option<BackendSchemaEvidence<'_>>,
    ) -> io::Result<CodexGeneration> {
        let timing = std::time::Instant::now();
        crate::debug_readiness_timing::record("publicReadinessBegin", timing);
        // Revalidate executable identity/publication on every generation; only compilation is cached.
        let (schema, payload_schemas) = match schema {
            Some(evidence) => {
                let digest = self
                    .publish_native_schema(BackendSchemaEvidence {
                        running_executable: evidence.running_executable,
                        export: evidence.export,
                    })
                    .await?;
                crate::debug_readiness_timing::record("publicSchemaVerified", timing);
                let compiled = self.payload_cache.resolve(evidence.export.bundle());
                crate::debug_readiness_timing::record("publicValidatorsResolved", timing);
                (Some(digest), compiled)
            }
            None => (None, None),
        };
        let admitted_schemas = if self.native_digest.as_ref() == schema.as_ref() {
            payload_schemas
        } else {
            None
        };
        let generation = self.publication.ready(at, schema, admitted_schemas)?;
        crate::debug_readiness_timing::record("publicGenerationPublished", timing);
        Ok(generation)
    }
    async fn publish_native_schema(
        &self,
        evidence: BackendSchemaEvidence<'_>,
    ) -> io::Result<SchemaDigest> {
        if evidence.export.executable() != evidence.running_executable {
            return Err(io::Error::other(
                "schema executable differs from running backend",
            ));
        }
        let observed = codex_native_integration::executable_identity(
            evidence.running_executable.canonical_path(),
        )
        .await
        .map_err(io::Error::other)?;
        if &observed != evidence.running_executable {
            return Err(io::Error::other(
                "schema executable changed before publication",
            ));
        }
        // Publication precedes the directory announcement; readers never receive a missing bundle.
        super::schema_bundle_retention::publish_in_use_schema_bundle(
            &self.directory,
            evidence.export.bundle(),
        )?;
        let mut encoded = String::from("sha256:");
        for byte in evidence.export.bundle().digest() {
            use std::fmt::Write as _;
            write!(encoded, "{byte:02x}").map_err(io::Error::other)?;
        }
        SchemaDigest::try_from(encoded).map_err(io::Error::other)
    }
    pub fn backend_unavailable(
        &mut self,
        at: ObservationTimestamp,
        reason: NonEmptyText,
    ) -> io::Result<()> {
        self.publication.unavailable(at, reason)
    }
}
