
const dbName = process.env.MONGO_DB;
const appUser = process.env.MONGO_APP_USERNAME;
const appPassword = process.env.MONGO_APP_PASSWORD;

const store = db.getSiblingDB(dbName);

store.createUser({
  user: appUser,
  pwd: appPassword,
  roles: [{ role: "readWrite", db: dbName }],
});

store.createCollection("hazard_events");
store.hazard_events.createIndex({ occurred_at: -1 });
store.hazard_events.createIndex({ ingested_at: -1 });
store.hazard_events.createIndex({ hazard_type: 1, occurred_at: -1 });
store.hazard_events.createIndex({ source: 1, source_ref_id: 1 }, { unique: true });

const volcanoes = [
  { _id: "VOL-MERAPI",          name: "Gunung Merapi",             province: "DI Yogyakarta / Jawa Tengah", latitude: -7.5407,  longitude: 110.4457 },
  { _id: "VOL-SEMERU",          name: "Gunung Semeru",             province: "Jawa Timur",                  latitude: -8.1077,  longitude: 112.9224 },
  { _id: "VOL-SINABUNG",        name: "Gunung Sinabung",           province: "Sumatera Utara",              latitude:  3.1700,  longitude:  98.3920 },
  { _id: "VOL-KELUD",           name: "Gunung Kelud",              province: "Jawa Timur",                  latitude: -7.9320,  longitude: 112.3080 },
  { _id: "VOL-BROMO",           name: "Gunung Bromo",              province: "Jawa Timur",                  latitude: -7.9425,  longitude: 112.9530 },
  { _id: "VOL-AGUNG",           name: "Gunung Agung",              province: "Bali",                        latitude: -8.3430,  longitude: 115.5080 },
  { _id: "VOL-RAUNG",           name: "Gunung Raung",              province: "Jawa Timur",                  latitude: -8.1250,  longitude: 114.0420 },
  { _id: "VOL-KRAKATAU",        name: "Gunung Anak Krakatau",      province: "Lampung",                     latitude: -6.1020,  longitude: 105.4230 },
  { _id: "VOL-MARAPI",          name: "Gunung Marapi",             province: "Sumatera Barat",              latitude: -0.3810,  longitude: 100.4730 },
  { _id: "VOL-LEWOTOBI",        name: "Gunung Lewotobi Laki-laki", province: "Nusa Tenggara Timur",         latitude: -8.5420,  longitude: 122.7750 },
  { _id: "VOL-ILE-LEWOTOLOK",   name: "Gunung Ile Lewotolok",      province: "Nusa Tenggara Timur",         latitude: -8.2720,  longitude: 123.5050 },
  { _id: "VOL-DUKONO",          name: "Gunung Dukono",             province: "Maluku Utara",                latitude:  1.6930,  longitude: 127.8940 },
  { _id: "VOL-IBU",             name: "Gunung Ibu",                province: "Maluku Utara",                latitude:  1.4880,  longitude: 127.6300 },
  { _id: "VOL-GAMALAMA",        name: "Gunung Gamalama",           province: "Maluku Utara",                latitude:  0.8090, longitude: 127.3300 },
  { _id: "VOL-SLAMET",          name: "Gunung Slamet",             province: "Jawa Tengah",                 latitude: -7.2420,  longitude: 109.2080 },
];

store.createCollection("volcano_reference");
store.volcano_reference.insertMany(volcanoes);

print(`init done: db=${dbName} user=${appUser} volcano_reference=${store.volcano_reference.countDocuments()}`);
