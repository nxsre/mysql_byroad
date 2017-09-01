var client = $.cookie('client');
if (!client) {
  $("#client option:first").attr('selected', true);
  $.cookie('client', $('#client').val(), { path: '/' });
} else {
  var flag = false;
  $('#client option').each(function () {
    if ($(this).val() == client) {
      $(this).attr('selected', true);
      flag = true;
      return false;
    }
  });
  if (!flag) {
    $("#client option:first").attr('selected', true);
    $.cookie('client', $('#client').val(), { path: '/' });
  }
}

function removeField(obj) {
  $(obj).closest('.form-group').parent().remove();
}

function removeFieldDiv(obj) {
  $(obj).parent().parent().remove();
}

function addRightField(schema, table, column) {
  var field = schema + '@@' + table + '@@' + column;
  var divid = schema + '-' + table;
  var data = '<div class="col-md-4" style="margin-bottom:5px;">\
  <div class="form-group">\
  <div class="input-group">\
  <input value="'+ column + '" readonly type="text" class="form-control">\
  <input name="fields" type="hidden" value="'+ field + '">\
  </div>\
  <div class="checkbox"><label><input type="checkbox" name="' + field + '" value="1"></input>是否推送值<label></div> \
  <a href="javascript:void(0)" onclick="removeField(this)"><span class="glyphicon glyphicon-remove" aria-hidden="true"></span></a>\
  </div>\
  </div>';
  addRightDiv(schema, table);
  if (!checkRightField(schema, table, column)) {
    $('#' + divid).find('.rowCont').append(data);
  }
}

function addRightDiv(schema, table) {
  var field = schema + '-' + table;
  var data = '<div class="col-md-12 right-field-div" id="' + field + '">\
  <h3><input class="form-control input-lg schema-name" onchange="changeFieldValue(this)" value="' + schema + '"></input> \
  <input class="form-control input-lg table-name" onchange="changeFieldValue(this)" value="' + table + '"></input>\
  <a href="javascript:void(0)" onclick="removeFieldDiv(this)" class="btn btn-lg">\
  <span class="glyphicon glyphicon-remove" aria-hidden="true"></span>\
  </a>\
  </h3>\
  <div class="row rowCont"></div></div>';
  var d = document.getElementById(field);
  if (!d) {
    $('#rightdiv').append(data);
  }
}

function checkRightField(schema, table, column) {
  var field = schema + '-' + table;
  var d = document.getElementById(field);
  return $(d).find(':text').is($(':text[value=' + column + ']'));
}

function changeFieldValue(obj) {
  var pardiv = $(obj).parents('.right-field-div')[0];
  var schema_val = $(pardiv).find('.schema-name').val();
  var table_val = $(pardiv).find('.table-name').val();
  var newVal = schema_val + '@@' + table_val;
  var hiddenobj = $(pardiv).find(':hidden');
  var checkboxs = $(pardiv).find(':checkbox')
  $(hiddenobj).each(function () {
    var fieldVal = $(this).prev().val();
    $(this).val(newVal + '@@' + fieldVal);
  });
  $(checkboxs).each(function () {
    var fieldVal = $(this).attr('name').split('@@')[2];
    $(this).attr('name', newVal + '@@' + fieldVal)
  });
}

function addTask() {
  if (confirm('确认添加？')) {
    if ($('#auditUser').val() == '') {
      alert('审核人不能为空!')
      return
    }
    var options = {
      url: '/task',
      type: 'post',
      dataType: 'json',
      data: $('form').serialize(),
      success: function (data) {
        alert(data.Message);
      },
      error: function (data) {
        if (data.status == 422) {
          alert("任务数据格式错误");
        } else {
          alert('添加失败');
        }
      }
    };
    $.ajax(options);
    return false;
  }
}

function modifyTask() {
  if (confirm('确认修改？')) {
    var options = {
      url: '/task',
      type: 'put',
      dataType: 'json',
      data: $('#form').serialize(),
      success: function (data) {
        alert(data.Message);
      },
      error: function (data) {
        if (data.status == 422) {
          alert("任务数据格式错误");
        } else {
          alert('修改失败');
        }
      }
    };
    $.ajax(options);
    return false;
  }
}

function modifyTaskFields() {
  if (confirm('确认提交审核？')) {
    if ($('#auditUser').val() == '') {
      alert('审核人不能为空!')
      return
    }
    var options = {
      url: '/task-fields?isUpdateFields=1',
      type: 'put',
      dataType: 'json',
      data: $('#form').serialize(),
      success: function (data) {
        alert(data.Message);
      },
      error: function (data) {
        if (data.status == 422) {
          alert("任务数据格式错误");
        } else {
          alert('修改失败');
        }
      }
    };
    $.ajax(options);
    return false;
  }
}

function deleteTask(taskid) {
  if (confirm("确认删除？")) {
    var option = {
      type: 'delete',
      url: '/task/' + taskid,
      dataType: 'json',
      success: function (data) {
        if (data.Error) {
          alert(data.Message)
        }
        location.reload();
      },
      error: function (data) {
        alert("操作失败");
      }
    };
    $.ajax(option);
  }
}

function changeTaskStat(taskid, stat) {
  var option = {
    type: 'post',
    url: '/task/changeStat/' + taskid,
    dataType: 'json',
    data: { "stat": stat },
    success: function (data) {
      if (data.Error) {
        alert(data.Message)
      }
      location.reload();
    },
    error: function (data) {
      alert("操作失败")
    }
  };
  $.ajax(option);
}

function changePushState(taskid, stat) {
  var option = {
    type: 'post',
    url: '/task/changePushState/' + taskid,
    dataType: 'json',
    data: { "stat": stat },
    success: function (data) {
      if (data.Error) {
        alert(data.Message)
      }
      location.reload();
    },
    error: function (data) {
      alert("操作失败")
    }
  };
  $.ajax(option);
}

function copyTaskDialog(taskid, taskName) {
  $('#copyTaskModal [name="taskid"]').val(taskid);
  $('#copyTaskModal [name="taskName"]').val(taskName);
  $('#copyTaskModal').modal('toggle');
}

function copyTask() {
  var taskid = $('#copyTaskModal [name="taskid"]').val();
  if (confirm('确认复制？')) {
    var options = {
      url: '/task/copy/' + taskid,
      type: 'post',
      dataType: 'json',
      data: $('#copyTaskForm').serialize(),
      success: function (data) {
        alert(data.Message);
      },
      error: function (data) {
        if (data.status == 422) {
          alert("任务数据格式错误");
        } else {
          alert('复制失败');
        }
      }
    };
    $.ajax(options);
    return false;
  }
}

function doUpdateTaskUser() {
  if (confirm('确认更新该任务的用户？')) {
    var taskid = $('#id').val()
    var createUser = $('#new-createuser').val()
    $.ajax({
      url:'/task/updateuser/'+id,
      type:'post',
      data: {
        taskid: taskid,
        createUser: createUser,
      },
      success: function (data) {
        alert(data.Message)
      }
    })
  }
}


function doAddUser() {
  if (confirm('确认添加该用户？')) {
    var user = {
      'username': $('#username').val(),
      'role': parseInt($('#role').val()),
      'mail': $('#mail').val(),
    }
    post('/user', user, function(data) {
      alert(data.Message)
      if (data.Error) {
      } else {
        location.href = '/user-list'      
      }
    })
  }
}

function doDeleteUser(id) {
  if (confirm('确认删除该用户？')) {
    var user = {
      'id': id,
    }
    del('/user', user, function(data) {
      alert(data.Message)
      if (data.Error) {
      } else {
        location.href = '/user-list'      
      }
    })
  }
}

function doUpdateUser() {
  if (confirm('确认更新该用户？')) {
    var id = $('#id').val()
    var user = {
      'id': parseInt(id),
      'role': parseInt($('#role').val()),
    }
    put('/user', user, function(data) {
      alert(data.Message)
      if (data.Error) {
      } else {
        location.href = '/user-edit/'+id     
      }
    })
  }
}

function getDialogTask(el, id) {
  if ($('#myModal-'+id).length > 0) {
    $('#myModal-'+id).modal('toggle')
  } else {
    $.get('/task-dialog/'+id, function(data) {
      $(el).after(data)
      $('#myModal-'+id).modal('toggle')
    })
  }
}



function getDialogAudit(el, id) {
  if ($('#myModal-'+id).length > 0) {
    $('#myModal-'+id).modal('toggle')
  } else {
    $.get('/audit-dialog/'+id, function(data) {
      $(el).after(data)
      $('#myModal-'+id).modal('toggle')
    })
  }
}

function auditApprove(id) {
  if (confirm('确认同意该任务？')) {
    $.ajax({
      url: '/audit/approve/'+id,
      type: 'post',
      success: function(data) {
        alert(data.Message)
        window.location.reload()
      }
    })
  }
}

function auditDeny(id) {
  if (confirm('确认不同意该任务？')) {
    $.ajax({
      url: '/audit/deny/'+id,
      type: 'post',
      success: function(data) {
        alert(data.Message)
        window.location.reload()
      }
    })
  }
}

function enableAudit(auditid) {
  if (confirm('确认启用该任务？')) {
    $.ajax({
      url: '/audit/enable/'+auditid,
      type: 'post',
      success: function(data) {
        alert(data.Message)
        window.location.reload()
      }
    })
  }
}

function addCategory(obj) {
	if ($('#category_input').length > 0 ) {
		$(obj).attr('class', 'glyphicon glyphicon-plus');
		$('#category_input').replaceWith(categorySelector);
	}else if ($('#category_select').length > 0 ) {
		$(obj).attr('class', 'glyphicon glyphicon-circle-arrow-left');		
		$('#category_select').replaceWith('<input id="category_input" class="form-control" type="text" name="category"></input>');		
	}
}
var categorySelector;
$(function () {
  categorySelector = $('#category_select').prop("outerHTML");
  var instanceName = $('#client').val() || 'default'
  if (location.pathname.startsWith('/addtask')) {
    if ($('#category_select option[value="'+instanceName+'"]').length > 0) {
      $('#category_select option[value="'+instanceName+'"]').attr('selected', true)
    } else {
      var optionHtml = '<option selected value="'+instanceName+'">'+instanceName+'</option>'
      $('#category_select').append(optionHtml)
    }
  }
  $("#updateTaskFieldsBtn").popover({
    trigger: 'hover',
    title: '更新订阅字段',
    placement: 'top',
    html: true,
    content: '<dd><dl>更新订阅字段需要提交审核</dl></dd>',
  });

  $("#updateTaskBtn").popover({
    trigger: 'hover',
    title: '更新任务信息',
    placement: 'top',
    html: true,
    content: '<dd><dl>更新任务信息不需要提交审核</dl></dd>',
  });

  $("#addTaskFieldsBtn").popover({
    trigger: 'hover',
    title: '新增订阅字段',
    placement: 'top',
    html: true,
    content: '<dd><dl>新增订阅字段需要提交审核</dl></dd>',
  });

  $("#pack-help").popover({
    trigger: 'hover',
    title: '数据封装协议',
    html: true,
    content: '<dd><dt>默认</dt><dl>旁路系统原有格式: 消息内容从post请求的body中读取。</dl><dl>消费方处理完成后返回 success</dl><dt>消息中心推送协议</dt><dl>使用消息中心的推送协议进行数据封装: message=POST["message"], jobid=GET["jobid"], retry_times=GET["retry_times"]</dl><dl>消费方处理完成后返回{"status": 1}</dl></dd>',
  });

  $("#regexp-help").popover({
    trigger: 'hover',
    title: '正则表达式',
    html: true,
    content: '<dd><dt>支持正则表达式</dt><dl>数据库名和表名都可以使用正则表达式</dl><dl>默认会在表达式前后添加<strong>"^"</strong>和<strong>"$"</strong>符号</dl><dl>之前的<strong>*</strong>的效果同现在的<strong>([\\w]+)</strong>一样</dl><dt>',
  });

  $('#client').change(function () {
    $.cookie('client', $(this).val(), { path: '/' });
    location.reload();
  });

  $('a[data-toggle="collapse"]').click(function () {
    $(this).find('.glyphicon').toggleClass("glyphicon-triangle-bottom glyphicon-triangle-right");
    $(this).next().collapse('toggle');
  });

  $('.click-show').click(function () {
    $(this).toggleClass('click-show');
    $(this).parent().siblings().children().toggleClass('click-show');
  });

  $('#schema-search').jSearch({
    selector: '#column-list',
    child: 'li .schema-for-search',
    minValLength: 0,
    Found: function (elem) {
      $(elem).next().children().show();
    },
    NotFound: function (elem) {
      $(elem).parent().hide();
    },
    After: function (t) {
      if (!t.val().length) $('#column-list>li').show();
    }
  });
});

function ajax(method, url, data, callback) {
  $.ajax({
    url: url,
    type: method,
    data: JSON.stringify(data),
    contentType:'application/json',
    success: function (data) {
      callback(data)
    },
    error: function(jqXHR) {
      alert(jqXHR.responseText)
    }
  })
}

function get(url, data, callback) {
  return ajax('get', url, data, callback)
}

function post(url, data, callback) {
  return ajax('post', url, data, callback)
}

function put(url, data, callback) {
  return ajax('put', url, data, callback)
}

function del(url, data, callback) {
  return ajax('delete', url, data, callback)
}