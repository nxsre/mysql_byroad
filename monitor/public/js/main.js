var inspector = $.cookie('inspector');
if (!inspector) {
  $("#inspector option:first").attr('selected', true);
  $.cookie('inspector', $('#inspector').val(), { path: '/' });
} else {
  var flag = false;
  $('#inspector option').each(function () {
    if ($(this).val() == inspector) {
      $(this).attr('selected', true);
      flag = true;
      return false;
    }
  });
  if (!flag) {
    $("#inspector option:first").attr('selected', true);
    $.cookie('inspector', $('#inspector').val(), { path: '/' });
  }
}

function removeField(obj) {
  $(obj).closest('.form-group').parent().remove();
}

function removeFieldDiv(obj) {
  $(obj).parent().parent().remove();
}

function addRightField(schema, table, column) {
  var field = schema + '.' + table + '.' + column;
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
      <h3><input class="form-control input-lg" onchange="changeFieldValue(this)" value="' + schema + '.' + table + '"></input> \
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
  var newVal = $(obj).val();
  var pardiv = $(obj).parents(".right-field-div")[0];
  var hiddenobj = $(pardiv).find(":hidden");
  $(hiddenobj).each(function () {
    var fieldVal = $(this).prev().val();
    $(this).val(newVal + "." + fieldVal);
  });
}

function addTask() {
  if (confirm('确认添加？')) {
    var options = {
      url: '/task',
      type: 'post',
      dataType: 'json',
      data: $('#form').serialize(),
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

function deleteTask(taskid) {
  if (confirm("确认删除？")) {
    var option = {
      type: 'delete',
      url: '/task/' + taskid,
      dataType: 'json',
      success: function (data) {
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
    url: 'task/changeStat/' + taskid,
    dataType: 'json',
    data: { "stat": stat },
    success: function (data) {
      location.reload();
    },
    error: function (data) {
      alert("操作失败");
    }
  };
  $.ajax(option);
}

var columnMap = {};

function getTables(schema) {
  $('#' + schema).find('.glyphicon').toggleClass('glyphicon-triangle-bottom glyphicon-triangle-right');
  if (columnMap[schema]) {
    $('#' + schema).next().collapse('toggle');
  } else {
    $.get('/tables', { schema: schema }, function (response) {
      columnMap[schema] = response;
      $('#' + schema).after(response);
      $('#' + schema).next().collapse('toggle');
    });
  }
}

function getColumns(schema, table) {
  var aid = '#' + schema + table;
  $(aid).find('.glyphicon').toggleClass('glyphicon-triangle-bottom glyphicon-triangle-right');
  if (columnMap[schema][table]) {
    $(aid).next().collapse('toggle');
  } else {
    $.get('/columns', { schema: schema, table: table }, function (response) {
      $(aid).after(response);
      $(aid).next().collapse('toggle');
      columnMap[schema] = {};
      columnMap[schema][table] = response;
    });
  }
}

$(function () {
  $('#pack-help').popover({
    trigger: 'hover',
    title: '数据封装协议',
    html: true,
    content: '<dd><dt>默认</dt><dl>旁路系统原有格式: 消息内容从post请求的body中读取。</dl><dl>消费方处理完成后返回 success</dl><dt>消息中心推送协议</dt><dl>使用消息中心的推送协议进行数据封装: message=POST["message"], jobid=GET["jobid"], retry_times=GET["retry_times"]</dl><dl>消费方处理完成后返回{"status": 1}</dl></dd>',
  });

/*  $('a[data-toggle="collapse"]').click(function () {
    $(this).find('.glyphicon').toggleClass("glyphicon-triangle-bottom glyphicon-triangle-right");
    $(this).next().collapse('toggle');
  });*/

  $('#search').jSearch({
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

  $('.click-show').click(function () {
    $(this).toggleClass('click-show');
    $(this).parent().siblings().children().toggleClass('click-show');
  });

  $('#inspector').change(function () {
    $.cookie('inspector', $(this).val(), { path: '/' });
    location.reload();
  });
});


