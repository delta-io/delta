// @ts-check
import { defineConfig } from "astro/config";
import starlight from "@astrojs/starlight";

// https://astro.build/config
export default defineConfig({
  site: "https://delta-docs-incubator.netlify.app/",
  image: {
    service: {
      entrypoint: "astro/assets/services/sharp",
    },
  },
  integrations: [
    starlight({
      customCss: ["./src/styles/custom.css"],
      title: "Delta Lake",
      social: {
        github: "https://github.com/delta-io/delta",
      },
      editLink: {
        baseUrl:
          "https://github.com/jakebellacera/db-site-staging/tree/main/sites/delta-docs",
      },
      lastUpdated: true,
      logo: {
        light: "./src/assets/delta-lake-logo-light.svg",
        dark: "./src/assets/delta-lake-logo-dark.svg",
        replacesTitle: true,
      },
      sidebar: [
        { label: "Welcome", link: "/" },
        {
          label: "Apache Spark connector",
          items: [
            {
              label: "Quickstart",
              link: "/apache-spark-connector/quickstart/",
            },
            {
              label: "Table batch reads and writes",
              link: "/apache-spark-connector/table-batch-reads-and-writes/",
            },
            {
              label: "Table streaming reads and writes",
              link: "/apache-spark-connector/table-streaming-reads-and-writes/",
            },
            {
              label: "Table deletes, updates, and merges",
              link: "/apache-spark-connector/table-deletes-updates-and-merges/",
            },
            {
              label: "Change data feed",
              link: "/apache-spark-connector/change-data-feed/",
            },
            {
              label: "Table utility commands",
              link: "/apache-spark-connector/table-utility-commands/",
            },
            {
              label: "Constraints",
              link: "/apache-spark-connector/constraints/",
            },
            {
              label: "How does Delta Lake manage feature compatibility?",
              link: "/apache-spark-connector/how-does-delta-lake-manage-feature-compatibility/",
            },
            {
              label: "Delta default column values",
              link: "/apache-spark-connector/delta-default-column-values/",
            },
            {
              label: "Delta column mapping",
              link: "/apache-spark-connector/delta-column-mapping/",
            },
            {
              label: "Use liquid clustering for Delta tables",
              link: "/apache-spark-connector/use-liquid-clustering-for-delta-tables/",
            },
            {
              label: "What are deletion vectors?",
              link: "/apache-spark-connector/what-are-deletion-vectors/",
            },
            {
              label: "Drop Delta table features",
              link: "/apache-spark-connector/drop-delta-table-features/",
            },
            {
              label: "Use row tracking for Delta tables",
              link: "/apache-spark-connector/use-row-tracking-for-delta-tables/",
            },
            {
              label: "Storage configuration",
              link: "/apache-spark-connector/storage-configuration/",
            },
            {
              label: "Delta type widening",
              link: "/apache-spark-connector/delta-type-widening/",
            },
            {
              label: "Universal Format (UniForm)",
              link: "/apache-spark-connector/universal-format-uniform/",
            },
            {
              label: "Read Delta Sharing Tables",
              link: "/apache-spark-connector/read-delta-sharing-tables/",
            },
            {
              label: "Concurrency control",
              link: "/apache-spark-connector/concurrency-control/",
            },
            {
              label: "Migration guide",
              link: "/apache-spark-connector/migration-guide/",
            },
            {
              label: "Best practices",
              link: "/apache-spark-connector/best-practices/",
            },
            {
              label: "Frequently asked questions (FAQ)",
              link: "/apache-spark-connector/faq/",
            },
            {
              label: "Optimizations",
              link: "/apache-spark-connector/optimizations/",
            },
          ],
        },
        {
          label: "Trino connector",
          link: "/trino-connector/",
        },
        {
          label: "Presto connector",
          link: "/presto-connector/",
        },
        {
          label:
            "Presto, Trino, and Athena to Delta Lake integration using manifests",
          link: "/presto-integration/",
        },
        {
          label: "AWS Redshift Spectrum connector",
          link: "/aws-redshift-spectrum-connector/",
        },
        {
          label: "Snowflake integration",
          link: "/snowflake-integration/",
        },
        {
          label: "Google BigQuery connector",
          link: "/bigquery-integration/",
        },
        {
          label: "Apache Flink connector",
          link: "/flink-integration/",
        },
        {
          label: "Delta more connectors",
          link: "/delta-more-connectors/",
        },
        {
          label: "Delta Kernel",
          link: "/delta-kernel/",
        },
        {
          label: "Delta Lake APIs",
          link: "/delta-lake-apis/",
        },
        {
          label: "Releases",
          link: "/releases/",
        },
        {
          label: "Delta Lake Resources",
          link: "/delta-resources/",
        },
        {
          label: "Delta table properties reference",
          link: "/table-properties/",
        },
        {
          label: "Contribute",
          link: "https://github.com/delta-io/delta/blob/master/CONTRIBUTING.md",
          attrs: { target: "_blank" },
        },
      ],
    }),
  ],
});
