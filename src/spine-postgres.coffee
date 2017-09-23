Spine        = require "spine"
Sequelize    = require "sequelize"
uuidV4       = require "uuid/v4"
errify       = require "errify"


capitalize = (str) -> str[0].toUpperCase() + str[1..]


class Postgres extends Spine.Model
  @uuid: -> "#{@className}_#{uuidV4()}"

  @connect: (server) ->
    {password} = server
    delete server.password

    server.dialect        ?= 'postgres'
    server.pool           ?= {}
    server.pool.max       ?= 5
    server.pool.min       ?= 0
    server.pool.idle      ?= 10000
    server.typeValidation ?= true

    Postgres._sequelize = new Sequelize(server.database, server.username, password, server)

  @connected: -> Boolean @connection()
  @connection: -> Postgres._sequelize

  @setup: (server) ->
    @connect server unless @connected()
    throw new Error "schema required" unless @schema

    @_schema = {}
    for attribute in @attributes
      type = @schema[attribute]
      @_schema[attribute] =
        if typeof type is "string" then type: Sequelize[type.toUpperCase()]
        else unless type           then type: Sequelize.STRING
        else                            type
    @_schema.id = @schema.id if @schema.id

    @table = @connection().define @className, @_schema

  @bootstrap: (callback) ->
    @table.sync().asCallback callback

  @parseRow: (row) ->
    attrs      = {}
    attrs[key] = row[key] for key in @attributes when key of row
    attrs.id   = row.id

    record     = new @ attrs
    record._record = row
    record

  @makeRecords: (rows) ->
    records = (@parseRow row for row in rows when row)
    records

  @find: (id, cb) ->
    ideally = errify cb

    await (@table.findById id).asCallback ideally defer row
    cb null, (row and @parseRow row)

  @findOne: (options = {}, cb = ->) ->
    (cb = options) and options = {} if typeof options is "function"
    ideally = errify cb

    await (@table.findOne options).asCallback ideally defer row
    cb null, (row and @parseRow row)

  @findAll: (options = {}, cb = ->) ->
    (cb = options) and options = {} if typeof options is "function"
    ideally = errify cb

    await (@table.findAll options).asCallback ideally defer rows
    cb null, @makeRecords rows

  @findMany: (ids, cb = ->) ->
    ideally = errify cb
    query =
      where: id: ids

    await (@table.findAll query).asCallback ideally defer rows
    cb null, @makeRecords rows

  @findAllByAttribute: (key, value, options = {}, cb = ->) ->
    (cb = options) and options = {} if typeof options is "function"
    ideally = errify cb

    name   = "findAllBy#{capitalize key}"
    method = @[name]
    return callback new Error "@#{name} not implemented" unless method?
    await method value, options, ideally defer rows
    cb null, @makeRecords rows

  @findAllById: (key, value, options, cb = ->) ->
    return @findMany value, cb if Array.isArray value
    @find value, cb

  @remove: (options = {}, cb = ->) ->
    (cb = options) and options = {} if typeof options is "function"
    (@table.destroy options).asCallback cb

  type: -> @constructor.className

  attributes: (hideId) ->
    result = super()
    delete result.id if hideId
    result

  exists: -> Boolean @_record

  save: (cb = ->) ->
    ideally = errify cb
    wasNew  = @isNew()
    {id}    = this

    if wasNew
      @[key] = value for key, value of @constructor.defaults when not @[key]?
      await (@constructor.table.create @attributes()).asCallback ideally defer _record
      @_record = _record
    else
      await (@_record.update (@attributes true)).asCallback ideally defer result

    cb null, this

  remove: (cb = ->) ->
    (@_record.destroy {force: true}).asCallback cb

  _makeIncrement: (attr, amount) ->
    Sequelize.literal "\"#{attr}\" + #{amount}"

  increment: (attr) ->
    @[attr] = @_makeIncrement attr, 1

  incrementAttributeByAmount: (attr, amount, callback) ->
    ideally = errify callback

    attrs = {}
    attrs[attr] = @_makeIncrement attr, amount

    await (@_record.update attrs).asCallback ideally defer result
    await @constructor.find @id, ideally defer updated
    @load updated

    callback null, this


module.exports = Postgres
