// Dashboard assets are imported with `with { type: "file" }`; each import is the
// asset path as a string (a path on disk from source, an embedded path in a
// compiled binary).
declare module "*/public/app.js" {
  const path: string;
  export default path;
}
declare module "*.css" {
  const path: string;
  export default path;
}
