// @ts-check
import { defineConfig } from "astro/config";
import starlight from "@astrojs/starlight";
import netlify from "@astrojs/netlify";

// https://astro.build/config
export default defineConfig({
  site: "https://delta-docs-incubator.netlify.app/",
  image: {
    service: {
      entrypoint: "astro/assets/services/sharp",
    },
  },
  adapter: netlify(),
  redirects: {
    "/latest/*": "/:splat",
    "/delta-intro.html": "/",
    "/delta-spark.html": "/",
    "/quick-start.html": "/quick-start",
    "/delta-batch.html": "/delta-batch",
    "/delta-streaming.html": "/delta-streaming",
    "/delta-update.html": "/delta-update",
    "/delta-change-data-feed.html": "/delta-change-data-feed",
    "/delta-utility.html": "/delta-utility",
    "/delta-constraints.html": "/delta-constraints",
    "/versioning.html": "/versioning",
    "/delta-default-columns.html": "/delta-default-columns",
    "/delta-column-mapping.html": "/delta-column-mapping",
    "/delta-clustering.html": "/delta-clustering",
    "/delta-deletion-vectors.html": "/delta-deletion-vectors",
    "/delta-drop-feature.html": "/delta-drop-feature",
    "/delta-row-tracking.html": "/delta-row-tracking",
    "/delta-spark-connect.html": "/delta-spark-connect",
    "/delta-storage.html": "/delta-storage",
    "/delta-type-widening.html": "/delta-type-widening",
    "/delta-uniform.html": "/delta-uniform",
    "/delta-sharing.html": "/delta-sharing",
    "/concurrency-control.html": "/concurrency-control",
    "/porting.html": "/porting",
    "/best-practices.html": "/best-practices",
    "/delta-faq.html": "/delta-faq",
    "/optimizations-oss.html": "/optimizations-oss",
    "/delta-trino-integration.html": "/delta-trino-integration",
    "/delta-presto-integration.html": "/delta-presto-integration",
    "/presto-integration.html": "/presto-integration",
    "/redshift-spectrum-integration.html": "/redshift-spectrum-integration",
    "/snowflake-integration.html": "/snowflake-integration",
    "/bigquery-integration.html": "/bigquery-integration",
    "/flink-integration.html": "/flink-integration",
    "/delta-more-connectors.html": "/delta-more-connectors",
    "/delta-kernel.html": "/delta-kernel",
    "/delta-standalone.html": "/delta-standalone",
    "/delta-apidoc.html": "/delta-apidoc",
    "/releases.html": "/releases",
    "/delta-resources.html": "/delta-resources",
    "/table-properties.html": "/table-properties",
  },
  integrations: [
    starlight({
      customCss: ["./src/styles/custom.css"],
      title: "Delta Lake",
      social: [
        {
          icon: "github",
          label: "GitHub",
          href: "https://github.com/delta-io/delta",
        },
      ],
      editLink: {
        baseUrl:
          "https://github.com/jakebellacera/db-site-staging/tree/main/sites/delta-docs",
      },
      lastUpdated: true,
      favicon: "/favicon.svg",
      logo: {
        light: "./src/assets/delta-lake-logo-light.svg",
        dark: "./src/assets/delta-lake-logo-dark.svg",
        replacesTitle: true,
      },
      sidebar: [
        { label: "Introduction", link: "/" },
        {
          label: "Apache Spark connector",
          collapsed: true,
          items: [
            {
              slug: "quick-start",
            },
            {
              slug: "delta-batch",
            },
            {
              slug: "delta-streaming",
            },
            {
              slug: "delta-update",
            },
            {
              slug: "delta-change-data-feed",
            },
            {
              slug: "delta-utility",
            },
            {
              slug: "delta-constraints",
            },
            {
              slug: "versioning",
            },
            {
              slug: "delta-default-columns",
            },
            {
              slug: "delta-column-mapping",
            },
            {
              slug: "delta-clustering",
            },
            {
              slug: "delta-deletion-vectors",
            },
            {
              slug: "delta-drop-feature",
            },
            {
              slug: "delta-row-tracking",
            },
            {
              slug: "delta-spark-connect",
            },
            {
              slug: "delta-storage",
            },
            {
              slug: "delta-type-widening",
            },
            {
              slug: "delta-uniform",
            },
            {
              slug: "delta-sharing",
            },
            {
              slug: "concurrency-control",
            },
            {
              slug: "porting",
            },
            {
              slug: "best-practices",
            },
            {
              slug: "delta-faq",
            },
            {
              slug: "optimizations-oss",
            },
          ],
        },
        {
          slug: "delta-trino-integration",
        },
        {
          slug: "delta-presto-integration",
        },
        {
          slug: "redshift-spectrum-integration",
        },
        {
          slug: "snowflake-integration",
        },
        {
          slug: "bigquery-integration",
        },
        {
          slug: "flink-integration",
        },
        {
          slug: "delta-more-connectors",
        },
        {
          slug: "delta-kernel",
        },
        {
          slug: "delta-standalone",
        },
        {
          slug: "delta-apidoc",
        },
        {
          slug: "releases",
        },
        {
          slug: "delta-resources",
        },
        {
          slug: "table-properties",
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
