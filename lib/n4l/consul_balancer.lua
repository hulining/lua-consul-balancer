--
--
--
--

-- 简单说明: 尝试使用 https://github.com/openresty/lua-resty-balancer 与 共享内存 来实现 upstream server 支持权重
-- 其中，权重的配置是通过注册服务时配置 https://github.com/hashicorp/consul/blob/v1.4.0/api/agent.go#L67-L70 传入的

-- Dependencies
local http = require "resty.http"
local balancer = require "ngx.balancer"
local json = require "cjson"
local resty_roundrobin = require "resty.roundrobin"

local WATCH_RETRY_TIMER = 0.5

local ok, new_tab = pcall(require, "table.new")
if not ok or type(new_tab) ~= "function" then
  new_tab = function (narr, nrec) return {} end
end

local _M = new_tab(0, 5) -- Change the second number.

_M.VERSION = "0.04"
_M._cache = {}

-- 校验 consul uri
local function _sanitize_uri(consul_uri)
  -- TODO: Ensure that uri has <proto>://<host>[:<port>] scheme
  return consul_uri
end

-- 定时器函数
local function _timer(...)
  local ok, err = ngx.timer.at(...)
  if not ok then
    ngx.log(ngx.ERR, "[FATAL] consul.balancer: failed to create timer: ", err)
  end
end

-- 验证 service 对象传入的参数描述
local function _validate_service_descriptor(service_descriptor)
  if type(service_descriptor) == "string" then
    service_descriptor = {
      name = service_descriptor,
      service = service_descriptor,
      tag = nil
    }
  elseif type(service_descriptor) == "table" then
    if service_descriptor.name == nil then
      return nil, "missing name field in service_descriptor"
    end
    if service_descriptor.service == nil then
      service_descriptor.service = service_descriptor.name
    end
  end
  return service_descriptor
end

-- 构建向 consul 请求的 uri
local function _build_service_uri(service_descriptor, service_index)
  local uri = _M._consul_uri .. "/v1/health/service/" .. service_descriptor.service
  local args = {
    index = service_index,
    wait = "5m"
  }
  if service_descriptor.dc ~= nil then
    args.dc = service_descriptor.dc
  end
  if service_descriptor.tag ~= nil then
    args.tag = service_descriptor.tag
  end
  if service_descriptor.near ~= nil then
    args.near = service_descriptor.near
  end
  if service_descriptor["node-meta"] ~= nil then
    args["node-meta"] = service_descriptor["node-meta"]
  end
  if service_descriptor.token ~= nil then
    args.token = service_descriptor.token
  end
  return uri .. "?" .. ngx.encode_args(args)
end

-- 将 response 解析为 service.upstreams 对象. 
-- TODO: 需要将返回转化为 rr_up 对象
local function _parse_service(response)
  if response.status ~= 200 then
    return nil, "bad response code: " .. response.status
  end
  local ok, content = pcall(function()
    return json.decode(response.body)
  end)
  if not ok then
    return nil, "JSON decode error"
  end
  if not response.headers["X-Consul-Knownleader"] or response.headers["X-Consul-Knownleader"] == "false" then
    return nil, "not trusting leaderless consul"
  end
  -- TODO: reuse some table?
  if not response.headers["X-Consul-Index"] then
    return nil, "missing consul index"
  end
  local upstreams = {}
  for k, v in pairs(content) do
    local passing = true
    local checks = v["Checks"]
    for i, c in pairs(checks) do
      if c["Status"] ~= "passing" then
        passing = false
      end
    end
    if passing then
      local s = v["Service"]
      local na = v["Node"]["Address"]
      
      local address = s["Address"] ~= "" and s["Address"] or na
      local port = s["Port"]
      local weight = s["Weights"]["Passing"]

      upstreams[address .. ":" .. port] = weight
      ngx.log(ngx.INFO, "consul.balancer: add " .. address .. ":" .. port .. " to upstreams, weight " .. weight)
    end
  end
  local rr_up = resty_roundrobin:new(upstreams)
  return rr_up
end


-- 更新 service 对象属性
-- TODO: 更新 rr_up 对象属性
local function _refresh(hc, uri)
  ngx.log(ngx.INFO, "consul.balancer: query uri: ", uri)
  local res, err = hc:request_uri(uri, {
    method = "GET"
  })
  if res == nil then
    ngx.log(ngx.ERR, "consul.balancer: failed to refresh upstreams: ", err)
    return nil, err
  end
  local rr_up, err = _parse_service(res)
  if err ~= nil then
    ngx.log(ngx.ERR, "consul.balancer: failed to parse consul response: ", err)
    return nil, err
  end
  return rr_up
end



-- watch 机制
-- signature must match nginx timer API
-- [x] TODO: 适配 rr_up 对象
local function _watch(premature, service_descriptor)
  if premature then
    return nil
  end
  service_descriptor, err = _validate_service_descriptor(service_descriptor)
  if err ~= nil then
    ngx.log(ngx.ERR, "consul.balancer: ", err)
    return nil
  end
  local hc = http:new()
  hc:set_timeout(360000) -- consul api has a default of 5 minutes for tcp long poll
  local service_index = 0
  ngx.log(ngx.NOTICE, "consul.balancer: started watching for changes in ", service_descriptor.name)
  while true do
    local uri = _build_service_uri(service_descriptor, service_index)
    local rr_up, err = _refresh(hc, uri)
    if rr_up == nil then
      ngx.log(ngx.ERR, "consul.balancer: failed while watching for changes in ", service_descriptor.name, " retry scheduled")
      _timer(WATCH_RETRY_TIMER, _watch, service_descriptor)
      return nil
    end
    package.loaded[service_descriptor.name] = rr_up
    -- TODO: Save only newer data from consul to reduce GC load
    ngx.log(ngx.INFO, "consul.balancer: persisted service ", service_descriptor.name,
                      " index: ", service_index, " content: ", json.encode(rr_up))
    ngx.sleep(5)
  end
end

-- 函数的主入口
function _M.watch(consul_uri, service_list)
  -- start watching on first worker only, skip for others (if shared storage provided)
  if _M.shared_cache and ngx.worker.id() > 0 then
    return
  end
  -- TODO: Reconsider scope for this variable.
  _M._consul_uri = _sanitize_uri(consul_uri)
  for k,v in pairs(service_list) do
    _timer(0, _watch, v)
  end
end

-- 轮询 balancer 进行负载均衡
-- [x] TODO: 取出 rr_up 对象,并进行负载均衡
function _M.round_robin(service_name)
  for k, v in pairs(package.loaded) do
    ngx.log(ngx.INFO, "consul.balancer: package loaded: ",k)
  end
  local rr_up = package.loaded[service_name]
  if rr_up == nil then
    ngx.log(ngx.ERR, "consul.balancer: no entry found for service: ", service_name)
    return ngx.exit(500)
  end
  
  local server = rr_up:find()
  local ok, err = balancer.set_current_peer(server)
  if not ok then
    ngx.log(ngx.ERR, "consul.balancer: failed to set the current peer: ", err)
    return ngx.exit(500)
  end
end


-- 设置共享内存名称
function _M.set_shared_dict_name(dict_name)
  _M.shared_cache = ngx.shared[dict_name]
  if not _M.shared_cache then
    ngx.log(ngx.ERR, "consul.balancer: unable to access shared dict ", dict_name)
    return ngx.exit(ngx.ERROR)
  end
end

return _M
