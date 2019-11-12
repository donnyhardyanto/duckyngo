/* eslint-disable camelcase,no-return-await */
/* Version 2.0 */
const mongodb = require('mongodb')
const moment = require('moment')
const ObjectId = require('mongodb').ObjectId
const duckytils = require('duckytils')
const lodash_string = require('lodash/string')
const crypto = require('crypto')

class Documents {
  constructor (url, documentName, expireAtFields, error_object_name, sequenceFields) {
    this.url = url
    this.documentName = documentName
    this.expireAtFields = expireAtFields
    this.error_object_name = error_object_name
    this.onChanged = null
    this.sequenceFields = sequenceFields
  }

  async createCollection () {
    const collection = await duckyngo.createCollection(this.url, this.documentName)
    if (this.expireAtFields) {
      for (let i = 0; i < this.expireAtFields.length; i++) {
        let p = {}
        let f = this.expireAtFields[i]
        p[f] = 1
        await duckyngo.createIndex(this.url, this.documentName, p, { expireAfterSeconds: 0 })
      }
    }
    if (this.sequenceFields) {
      for (let i = 0; i < this.sequenceFields.length; i++) {
        let p = {}
        let f = this.sequenceFields[i].field
        p[f] = 1
        await duckyngo.createIndex(this.url, this.documentName, p)
      }
    }
    try {
      await duckyngo.createIndex(this.url, '_counters', '_id')
    } catch (e) {
      // do nothing - suppress error
    }
    return collection
  }

  async add (document) {
    let d = Object.assign({}, document)
    d._add_timestamp = new Date(moment().toISOString(true))
    d._lastchanged_timestamp = d._add_timestamp
    if (this.sequenceFields) {
      for (let i = 0; i < this.sequenceFields.length; i++) {
        let f = this.sequenceFields[i]["field"]
        let c = await duckyngo.findOneAndUpsert(this.url, '_counters', { _id: this.documentName + f }, { _id: 1 }, { '$inc': { 'value': 1 } }, null)
        let c_as_string_length = c.value.toString().length
        let digits_as_number = parseInt(this.sequenceFields[i]["digits"])
        let n = digits_as_number - c_as_string_length
        let s = lodash_string.repeat('0', n)
        d[f] = this.sequenceFields[i].prefix + s + c.value
      }
    }
    return duckyngo.insertOne(this.url, this.documentName, d)
  }

  async add_all (documents) {
    for (let i = 0; i < documents.length; i++) {
      await this.add(documents[i])
    }
  }

  async findOneAndUpdate (whereKeyValues, orderKeyValues, updateKeyValues, projection) {
    return duckyngo.findOneAndUpdate(this.url, this.documentName, whereKeyValues, orderKeyValues, updateKeyValues, projection)
  }

  async updateManys (whereKeyValues, updateKeyValues, projection) {
    return duckyngo.updateMany(this.url, this.documentName, whereKeyValues, updateKeyValues, projection)
  }

  async findOneAndUpsert (whereKeyValues, orderKeyValues, updateKeyValues, projection) {
    return duckyngo.findOneAndUpsert(this.url, this.documentName, whereKeyValues, orderKeyValues, updateKeyValues, projection)
  }

  async updateSet (_id, setKeyValues, projection) {
    if ((typeof _id) !== 'object') _id = ObjectId(_id)
    setKeyValues._edit_timestamp = new Date(moment().toISOString())
    setKeyValues._lastchanged_timestamp = setKeyValues._edit_timestamp
    return this.findOneAndUpdate({ _id: _id }, null, { $set: setKeyValues }, projection)
  }

  // noinspection JSUnusedGlobalSymbols
  async update (_id, updateKeyValues, projection) {
    if ((typeof _id) !== 'object') _id = ObjectId(_id)
    let t = new Date(moment().toISOString())
    updateKeyValues = Object.assign({
      $set: {
        _edit_timestamp: t,
        _lastchanged_timestamp: t
      }
    }, updateKeyValues)
    return this.findOneAndUpdate({ _id: _id }, null, updateKeyValues, projection)
  }

  async delete (_id, projection) {
    if ((typeof _id) !== 'object') _id = ObjectId(_id)
    return duckyngo.deleteOne(this.url, this.documentName, { _id: _id }, projection)
  }

  async findOneAndUpdateSet (whereKeyValues, orderKeyValues, setKeyValues, projection) {
    return this.findOneAndUpdate(whereKeyValues, orderKeyValues, { $set: setKeyValues }, projection)
  }

  async findOneAndUpsertSet (whereKeyValues, orderKeyValues, setKeyValues, projection) {
    setKeyValues._edit_timestamp = new Date(moment().toISOString())
    setKeyValues._lastchanged_timestamp = setKeyValues._edit_timestamp
    return this.findOneAndUpsert(whereKeyValues, orderKeyValues, { $set: setKeyValues }, projection)
  }

  async findAndModify (whereKeyValues, setKeyValues, projection) {
    return this.findOneAndUpdate(whereKeyValues, null, { $set: setKeyValues }, projection)
  }

  async findOneAndDelete (whereKeyValues, projection) {
    return duckyngo.deleteOne(this.url, this.documentName, whereKeyValues, projection)
  }

  async view (_id, projection) {
    if ((typeof _id) !== 'object') _id = ObjectId(_id)
    return duckyngo.findOne(this.url, this.documentName, { _id: _id }, projection)
  }

  async view_must_exist_fatal (_id) {
    let o = await this.view(_id)
    let s = this.documentName
    if (this.error_object_name) s = this.error_object_name
    if (!o) throw new Error('FATAL_' + s + '_IS_NOT_FOUND_BECAUSE_NOT_EXIST')
    return o
  }

  async find_one (whereKeyValues, orderKeyValues, projection) {
    let l = await this.filter(whereKeyValues, orderKeyValues, 1, 0, projection)
    if (l.length === 0) return null
    return l[0]
  }

  // noinspection JSUnusedGlobalSymbols
  async find_one_raw (whereKeyValues, orderKeyValues, projection) {
    let l = await this.filter_raw(whereKeyValues, orderKeyValues, 1, 0, projection)
    if (l.length === 0) return null
    return l[0]
  }

  async list (whereKeyValues, orderKeyValues, projection) {
    return duckyngo.list(this.url, this.documentName, whereKeyValues, orderKeyValues, projection)
  }

  async filter (whereKeyValues, orderKeyValues, limit, offset, projection) {
    return duckyngo.filter(this.url, this.documentName, whereKeyValues, orderKeyValues, limit, offset, projection)
  }

  async list_raw (whereKeyValues, orderKeyValues, limit, projection) {
    return duckyngo.list(this.url, this.documentName, whereKeyValues, orderKeyValues, limit, projection)
  }

  async filter_raw (whereKeyValues, orderKeyValues, limit, offset, projection) {
    return duckyngo.filter(this.url, this.documentName, whereKeyValues, orderKeyValues, limit, offset, projection)
  }

  async count (whereKeyValues) {
    return duckyngo.count(this.url, this.documentName, whereKeyValues)
  }

  async sync_raw (whereKeyValues, client_lastchanged_timestamp, limit, projection) {
    let p = Object.assign({
      _lastchanged_timestamp: {
        $gte: client_lastchanged_timestamp
      }
    }, whereKeyValues)
    return this.list_raw(p, { _lastchanged_timestamp: 1 }, limit, projection)
  }

  // noinspection JSUnusedGlobalSymbols
  async sync (whereKeyValues, client_lastchanged_timestamp, limit, projection) {
    let t = moment(client_lastchanged_timestamp)
    t.subtract(1, 'seconds')
    return this.sync_raw(whereKeyValues, new Date(t.toISOString()), limit, projection)
  }

  async aggregate (pipeline, options, orderKeyValues, limit, offset) {
    return duckyngo.aggregate(this.url, this.documentName, pipeline, options, orderKeyValues, offset, limit)
  }
}

class DocumentsWithExpireAt extends Documents {
  constructor (url, documentName, defaultExpireAfterSeconds) {
    super(url, documentName, ['expireAt'])
    if (!defaultExpireAfterSeconds) defaultExpireAfterSeconds = 60 * 60 * 24 * 365 * 10
    this.defaultExpireAfterSeconds = defaultExpireAfterSeconds
  }

  async add (document, expireAfterSeconds) {
    if (!expireAfterSeconds) expireAfterSeconds = this.defaultExpireAfterSeconds
    if (!document.expireAt) document.expireAt = new Date(moment().add(expireAfterSeconds, 'seconds').toISOString())
    return super.add(document)
  }
}

class AsyncJob extends Documents {
  constructor (url, documentName, documentDoneName, documentErrorName) {
    super(url, documentName)
    this.doneDocument = new Documents(url, documentDoneName)
    this.errorDocument = new Documents(url, documentErrorName)
  }

  // noinspection JSUnusedGlobalSymbols
  async register_job (data_tag, name, parameters, progress) {
    let d = { name, parameters, progress: [{ text: progress }] }
    d = await super.add(d)
    duckytils.log(data_tag, 'info', 'Async job added: <' + d._id + '>:' + name + ': ' + progress, parameters)
    return d._id.toString()
  }

  // noinspection JSUnusedGlobalSymbols
  async progress (data_tag, job_id, progress_text, progress_data) {
    let d = await this.view(job_id)
    d.progress.push({ text: progress_text, data: progress_data })
    await this.updateSet(job_id, d)
    duckytils.log(data_tag, 'info', 'Async job progress: <' + d._id + '>:' + d.name + ': ' + progress_text, progress_data)
  }

  // noinspection JSUnusedGlobalSymbols
  async done (data_tag, job_id, result_data) {
    let d = await this.view(job_id)
    d.progress.push({ text: 'DONE', data: result_data })
    await this.delete(job_id)
    await this.doneDocument.add(d)
    duckytils.log(data_tag, 'info', 'Async job DONE: <' + d._id + '>:' + d.name + ': DONE', result_data)
  }

  async error (data_tag, job_id, error) {
    let d = await this.view(job_id)
    d.progress.push({ text: error.message, data: error.stack })
    await this.delete(job_id)
    await this.errorDocument.add(d)
    duckytils.log(data_tag, 'info', 'Async job ERROR: <' + d._id + '>:' + d.name + ': ' + error.message, error)
  }

  async register_job_promise (data_tag, p, name, parameters) {
    let self = this
    let job_id = await this.register_job(data_tag, name, parameters, 'START')
    return p.then((r) => {
      // noinspection JSIgnoredPromiseFromCall
      self.done(data_tag, job_id, r)
    }).catch((e) => {
      // noinspection JSIgnoredPromiseFromCall
      self.error(data_tag, job_id, e)
    })
  }
}

class DocumentsWithStates extends Documents {
  constructor (url, documentName, states, default_state, sequenceFields) {
    super(url, documentName, null, null, sequenceFields)
    this.states = states
    this.default_state = default_state
  }

  async add (document) {
    if (!document.state) document.state = this.default_state
    return super.add(document)
  }
}

class DocumentsWithStatesStandard extends DocumentsWithStates {
  constructor (url, documentName, sequenceFields) {
    super(url, documentName, DocumentsWithStatesStandard.states(), DocumentsWithStatesStandard.states().ACTIVE, sequenceFields)
  }

  static states () {
    return {
      ACTIVE: 'ACTIVE',
      DISABLED: 'DISABLED',
      DELETED: 'DELETED'
    }
  }

  static () {
    return DocumentsWithStatesStandard.states()
  }

  async delete (_id) {
    let t = new Date(moment().toISOString())
    return await super.updateSet(_id, {
      state: this.constructor.states().DELETED,
      _delete_timestamp: t,
      _lastchanged_timestamp: t
    })
  }

  // noinspection JSUnusedGlobalSymbols
  async true_delete (_id) {
    return await super.delete(_id)
  }

  async view_must_exist_fatal (_id) {
    let o = await super.view_must_exist_fatal(_id)
    if (o.state === DocumentsWithStatesStandard.states().DELETED) throw new Error('FATAL_IS_NOT_FOUND_BECAUSE_DELETED')
    return o
  }

  async list (whereKeyValues, orderKeyValues) {
    let w = Object.assign({ state: { $ne: this.constructor.states().DELETED } }, whereKeyValues)
    return super.list(w, orderKeyValues)
  }

  async filter (whereKeyValues, orderKeyValues, limit, offset) {
    let w = Object.assign({ state: { $ne: this.constructor.states().DELETED } }, whereKeyValues)
    return super.filter(w, orderKeyValues, limit, offset)
  }

  async count (whereKeyValues) {
    let w = Object.assign({ state: { $ne: this.constructor.states().DELETED } }, whereKeyValues)
    return super.count(w)
  }
}

class Users extends DocumentsWithStatesStandard {
  constructor (url, userDocumentName, userPasswordDocumentName, userPasswordDocumentUserIdFieldName) {
    super(url, userDocumentName)
    this.userPasswordDocumentUserIdFieldName = userPasswordDocumentUserIdFieldName
    this.passwords = new Documents(url, userPasswordDocumentName)
  }

  async createCollection () {
    await super.createCollection()
    await this.passwords.createCollection()
  }

  async add (document, password_hash) {
    let r = await super.add(document)
    if (password_hash) {
      let s = {}
      s[this.userPasswordDocumentUserIdFieldName] = r._id.toString()
      s.hash = password_hash
      await this.passwords.add(s)
    }
    return r
  }

  async add_all (documents, hash_algorithm_name = 'sha256') {
    for (let i = 0; i < documents.length; i++) {
      const hash = crypto.createHash(hash_algorithm_name)
      hash.update(documents[i]['login_password'])
      let password_hash = hash.digest('base64')
      delete documents[i]['login_password']
      await this.add(documents[i], password_hash)
    }
  }

  async viewPassword (user_id) {
    if ((typeof user_id) === 'object') user_id = user_id.toString()
    let r = {}
    r[this.userPasswordDocumentUserIdFieldName] = user_id
    const p = await this.passwords.list_raw(r)
    if (p.length === 0) throw new Error('FATAL_USER_HAS_NO_PASSWORD_RECORD')
    return p[0]
  }

  // noinspection JSUnusedGlobalSymbols
  async checkPassword (user_id, password_hash) {
    const userPassword = await this.viewPassword(user_id)
    if (userPassword.hash !== password_hash) throw new Error('FAIL_INVALID_CREDENTIAL')
    return true
  }

  // noinspection JSUnusedGlobalSymbols
  async changePassword (user_id, new_password_hash) {
    const userPassword = await this.viewPassword(user_id)
    await this.passwords.updateSet(userPassword._id, { hash: new_password_hash })
  }
}

// noinspection JSUnusedGlobalSymbols
const duckyngo = {
  Documents,
  DocumentsWithExpireAt,
  DocumentsWithStates,
  DocumentsWithStatesStandard,
  Users,
  AsyncJob,
  connections: [],
  connect: async function (url) {
    let c = duckyngo.connections[url.hash]
    if (!c) {
      return mongodb.MongoClient.connect(url.auth, {
        useNewUrlParser: true,
        useUnifiedTopology: true
      }).then((client) => {
        duckyngo.connections[url.hash] = client
        return client
      })
    } else {
      return c
    }
  },
  close: async function (url) {
    let c = duckyngo.connections[url.hash]
    if (c) c.close()
    delete duckyngo.connections[url.hash]
  },
  dropDatabase: async function (url) {
    return duckyngo.connect(url).then((client) => {
      const db = client.db(url.db)
      return db.dropDatabase()
    })
  },
  createCollection: async function (url, collectionName) {
    return duckyngo.connect(url).then((client) => {
      const db = client.db(url.db)
      return db.createCollection(collectionName)
    })
  },
  insertOne: async function (url, collectionName, doc) {
    return duckyngo.connect(url).then((client) => {
      const db = client.db(url.db)
      const collection = db.collection(collectionName)
      return collection.insertOne(doc, { w: 1, j: true }).then((r) => {
        if (r.result.ok !== 1) throw new Error('RESULT_OK_IS_NOT_1')
        return r.ops[0]
      })
    })
  },
  findOne: async function (url, collectionName, whereKeyValues, projection) {
    return duckyngo.connect(url).then((client) => {
      const db = client.db(url.db)
      const collection = db.collection(collectionName)
      return collection.findOne(whereKeyValues, projection)
    })
  },
  findOneAndUpdate: async function (url, collectionName, whereKeyValues, orderKeyValues, setKeyValues, projection) {
    return duckyngo.connect(url).then((client) => {
      const db = client.db(url.db)
      const collection = db.collection(collectionName)
      return collection.findOneAndUpdate(whereKeyValues, setKeyValues, {
        projection: projection,
        sort: orderKeyValues,
        returnOriginal: false,
        returnNewDocument: true,
        new: true
      }).then((r) => {
        return r.value
      })
    })
  },
  findOneAndUpsert: async function (url, collectionName, whereKeyValues, orderKeyValues, setKeyValues, projection) {
    return duckyngo.connect(url).then((client) => {
      const db = client.db(url.db)
      const collection = db.collection(collectionName)
      return collection.findOneAndUpdate(whereKeyValues, setKeyValues, {
        projection: projection,
        sort: orderKeyValues,
        upsert: true,
        returnOriginal: false,
        returnNewDocument: true,
        new: true
      }).then((r) => {
        return r.value
      })
    })
  },
  findAndModify: async function (url, collectionName, whereKeyValues, orderKeyValues, setKeyValues, projection) {
    return duckyngo.connect(url).then((client) => {
      const db = client.db(url.db)
      const collection = db.collection(collectionName)
      return collection.findAndModify(whereKeyValues, orderKeyValues, setKeyValues, {
        fields: projection,
        new: true
      }).then((r) => {
        return r.value
      })
    })
  },
  updateMany: async function (url, collectionName, whereKeyValues, setKeyValues, options) {
    return duckyngo.connect(url).then((client) => {
      const db = client.db(url.db)
      const collection = db.collection(collectionName)
      return collection.updateMany(whereKeyValues, setKeyValues, options).then((r) => {
        return r.value
      })
    })
  },
  createIndex: async function (url, collectionName, fieldOrSpec, options) {
    return duckyngo.connect(url).then((client) => {
      const db = client.db(url.db)
      const collection = db.collection(collectionName)
      return collection.createIndex(fieldOrSpec, options).then((r) => {
        return r.value
      })
    })
  },
  deleteOne: async function (url, collectionName, whereKeyValues, orderKeyValues, projection) {
    return duckyngo.connect(url).then((client) => {
      const db = client.db(url.db)
      const collection = db.collection(collectionName)
      return collection.findOneAndDelete(whereKeyValues, {
        projection,
        sort: orderKeyValues
      }).then((r) => {
        return r.value
      })
    })
  },
  list: async function (url, collectionName, whereKeyValues, orderKeyValues, limit, projection) {
    return duckyngo.connect(url).then((client) => {
      const db = client.db(url.db)
      const collection = db.collection(collectionName)
      let cursor = collection.find(whereKeyValues, projection)
      if (orderKeyValues) cursor = cursor.sort(orderKeyValues)
      if (limit) cursor = cursor.limit(limit)
      return cursor.toArray()
    })
  },
  filter: async function (url, collectionName, whereKeyValues, orderKeyValues, limit, offset, projection) {
    if (!offset) offset = 0
    if (!limit) limit = 0
    return duckyngo.connect(url).then((client) => {
      const db = client.db(url.db)
      const collection = db.collection(collectionName)
      let cursor = collection.find(whereKeyValues, projection)
      if (orderKeyValues) cursor = cursor.sort(orderKeyValues)
      if (offset) cursor = cursor.skip(offset)
      if (limit) cursor = cursor.limit(limit)
      return cursor.toArray()
    })
  },
  count: async function (url, collectionName, whereKeyValues) {
    return duckyngo.connect(url).then((client) => {
      const db = client.db(url.db)
      const collection = db.collection(collectionName)
      let cursor = collection.find(whereKeyValues)
      return cursor.count()
    })
  },
  aggregate: async function (url, collectionName, pipeline, options, orderKeyValues, offset, limit) {
    if (!offset) offset = 0
    if (!limit) limit = 0
    return duckyngo.connect(url).then((client) => {
      const db = client.db(url.db)
      const collection = db.collection(collectionName)
      let cursor = collection.aggregate(pipeline, options)
      if (orderKeyValues) cursor = cursor.sort(orderKeyValues)
      if (offset) cursor = cursor.skip(offset)
      if (limit) cursor = cursor.limit(limit)
      return cursor.toArray()
    })
  }
}

module.exports = duckyngo
