--liquibase formatted sql

--changeset mineberries:009-seed-test-data context:test
INSERT INTO app_user (user_id, nickname, password_hash) VALUES
    (1, 'buyer', 'password_hash_buyer'),
    (2, 'seller', 'password_hash_seller'),
    (3, 'admin', 'password_hash_admin');

INSERT INTO role (role_id, name, description) VALUES
    (1,  'BUYER',  'Покупатель'),
    (2,  'SELLER', 'Продавец'),
    (3,  'ADMIN',  'Администратор платформы');

INSERT INTO permission (permission_id, code, description) VALUES
    (1,  'order.create',           'Создание заказа со склада'),
    (2,  'order.view_own',         'Просмотр своих заказов'),
    (3,  'order.cancel_own',       'Отмена своего заказа до оплаты'),
    (4,  'order.track',            'Отслеживание статуса и курьера своего заказа'),
    (5,  'order.confirm_receipt',  'Подтверждение получения заказа'),
    (6,  'order.intake',           'Сдача товаров на склад'),
    (7,  'order.p2p',              'Отправка товаров другому игроку'),
    (8,  'lot.view_own',           'Просмотр своих лотов'),
    (9,  'payout.view_own',        'Просмотр своих выплат'),
    (10, 'payment.view_own',       'Просмотр своих платежей'),
    (11, 'profile.view_own',       'Просмотр своего профиля'),
    (12, 'profile.edit_own',       'Изменение своего профиля'),
    (13, 'profile.delete_own',     'Удаление своего аккаунта'),
    (14, 'notification.view_own',  'Просмотр своих уведомлений'),
    (15, 'dispute.open',           'Открытие спора'),
    (16, 'dispute.view_own',       'Просмотр своих споров'),
    (17, 'dispute.attach',         'Прикрепление файлов к спору'),
    (18, 'dispute.view_all',       'Просмотр всех споров'),
    (19, 'dispute.take',           'Взятие спора в работу'),
    (20, 'dispute.resolve',        'Удовлетворение спора'),
    (21, 'dispute.reject',         'Отклонение спора'),
    (22, 'compensation.view',      'Просмотр компенсаций'),
    (23, 'compensation.assign',    'Назначение компенсации'),
    (24, 'compensation.cancel',    'Отмена компенсации'),
    (25, 'user.view',              'Просмотр данных пользователей'),
    (26, 'user.edit',              'Изменение данных пользователей'),
    (27, 'user.block',             'Блокировка игроков'),
    (28, 'user.unblock',           'Снятие блокировки игроков'),
    (29, 'user.delete',            'Удаление пользователей'),
    (30, 'role.view',              'Просмотр ролей и прав'),
    (31, 'role.manage',            'Создание, изменение и удаление ролей'),
    (32, 'role.assign',            'Назначение и снятие ролей у пользователей'),
    (33, 'permission.assign',      'Назначение и снятие прав у ролей'),
    (34, 'catalog.view',           'Просмотр каталога'),
    (35, 'category.manage',        'Управление категориями'),
    (36, 'item.create',            'Создание карточки товара'),
    (37, 'item.edit',              'Изменение карточки товара'),
    (38, 'item.price.edit',        'Изменение цен товара'),
    (39, 'item.deactivate',        'Деактивация и активация товара'),
    (40, 'warehouse.view',         'Просмотр состояния складов'),
    (41, 'warehouse.manage',       'Создание и изменение складов'),
    (42, 'stock.view',             'Просмотр остатков на складах'),
    (43, 'stock.edit',             'Ручная корректировка остатков'),
    (44, 'inventory.view',         'Просмотр инвентаризаций и расхождений'),
    (45, 'inventory.run',          'Запуск инвентаризации'),
    (46, 'point.manage',           'Управление точками выдачи и приёма'),
    (47, 'courier.view',           'Просмотр курьеров и их позиций'),
    (48, 'courier.create',         'Добавление курьера'),
    (49, 'courier.edit',           'Изменение курьера'),
    (50, 'courier.deactivate',     'Деактивация курьера'),
    (51, 'courier.task.view',      'Просмотр заданий курьеров'),
    (52, 'courier.task.assign',    'Ручное назначение и отмена заданий курьера'),
    (53, 'orders.view_all',        'Просмотр всех заказов'),
    (54, 'orders.change_status',   'Ручное изменение статуса заказа'),
    (55, 'orders.cancel_any',      'Отмена любого заказа'),
    (56, 'payment.view_all',       'Просмотр всех платежей и выплат'),
    (57, 'payment.refund',         'Ручной возврат платежа'),
    (58, 'payout.retry',           'Повтор неудавшейся выплаты'),
    (59, 'logs.view',              'Просмотр журналов системы'),
    (60, 'incident.view',          'Просмотр инцидентов'),
    (61, 'incident.resolve',       'Закрытие инцидентов'),
    (62, 'admin_notification.view','Просмотр админских уведомлений');

INSERT INTO role_permission (role_id, permission_id) VALUES
    (1, 1), -- order.create
    (1, 2), -- order.view_own
    (2, 2), -- order.view_own
    (1, 3), -- order.cancel_own
    (2, 3), -- order.cancel_own
    (1, 4), -- order.track
    (2, 4), -- order.track
    (1, 5), -- order.confirm_receipt
    (2, 6), -- order.intake
    (2, 7), -- order.p2p
    (2, 8), -- lot.view_own
    (2, 9), -- payout.view_own
    (1, 10), -- payment.view_own
    (2, 10), -- payment.view_own
    (1, 11), -- profile.view_own
    (2, 11), -- profile.view_own
    (3, 11), -- profile.view_own
    (1, 12), -- profile.edit_own
    (2, 12), -- profile.edit_own
    (3, 12), -- profile.edit_own
    (1, 13), -- profile.delete_own
    (2, 13), -- profile.delete_own
    (1, 14), -- notification.view_own
    (2, 14), -- notification.view_own
    (3, 14), -- notification.view_own
    (1, 15), -- dispute.open
    (2, 15), -- dispute.open
    (1, 16), -- dispute.view_own
    (2, 16), -- dispute.view_own
    (1, 17), -- dispute.attach
    (2, 17), -- dispute.attach
    (3, 18), -- dispute.view_all
    (3, 19), -- dispute.take
    (3, 20), -- dispute.resolve
    (3, 21), -- dispute.reject
    (3, 22), -- compensation.view
    (3, 23), -- compensation.assign
    (3, 24), -- compensation.cancel
    (3, 25), -- user.view
    (3, 26), -- user.edit
    (3, 27), -- user.block
    (3, 28), -- user.unblock
    (3, 29), -- user.delete
    (3, 30), -- role.view
    (3, 31), -- role.manage
    (3, 32), -- role.assign
    (3, 33), -- permission.assign
    (1, 34), -- catalog.view
    (2, 34), -- catalog.view
    (3, 34), -- catalog.view
    (3, 35), -- category.manage
    (3, 36), -- item.create
    (3, 37), -- item.edit
    (3, 38), -- item.price.edit
    (3, 39), -- item.deactivate
    (3, 40), -- warehouse.view
    (3, 41), -- warehouse.manage
    (3, 42), -- stock.view
    (3, 43), -- stock.edit
    (3, 44), -- inventory.view
    (3, 45), -- inventory.run
    (3, 46), -- point.manage
    (3, 47), -- courier.view
    (3, 48), -- courier.create
    (3, 49), -- courier.edit
    (3, 50), -- courier.deactivate
    (3, 51), -- courier.task.view
    (3, 52), -- courier.task.assign
    (3, 53), -- orders.view_all
    (3, 54), -- orders.change_status
    (3, 55), -- orders.cancel_any
    (3, 56), -- payment.view_all
    (3, 57), -- payment.refund
    (3, 58), -- payout.retry
    (3, 59), -- logs.view
    (3, 60), -- incident.view
    (3, 61), -- incident.resolve
    (3, 62); -- admin_notification.view

INSERT INTO user_role (user_id, role_id, assigned_by) VALUES
    (1, 1, 3),    -- buyer  : BUYER
    (1, 2, 3),    -- buyer  : SELLER
    (2, 1, 3),    -- seller : BUYER
    (2, 2, 3),    -- seller : SELLER
    (3, 3, NULL); -- admin  : ADMIN

INSERT INTO point (point_id, world, x, y, z, label) VALUES (1,  'overworld', 0, 64, 0, 'Va warehouse');

INSERT INTO category (category_id, name) VALUES (1, 'Ресурсы');

INSERT INTO item (item_id, category_id, name, mc_material, sell_price, buyout_price) VALUES
    (1, 1, 'Алмаз', 'DIAMOND', 1000.00, 900.00);

-- Склад
INSERT INTO warehouse (warehouse_id, name, point_id, capacity) VALUES (1, 'Va warehouse', 1, 1000);

INSERT INTO stock_item (warehouse_id, item_id, quantity) VALUES (1, 1, 64);

-- Курьер
INSERT INTO courier (courier_id, mc_nickname, status, created_by) VALUES (1,  'courier_andy', 'IDLE', 3);

-- Сдвиг  счетчиков
SELECT setval(pg_get_serial_sequence('app_user',   'user_id'),       (SELECT max(user_id)       FROM app_user));
SELECT setval(pg_get_serial_sequence('role',       'role_id'),       (SELECT max(role_id)       FROM role));
SELECT setval(pg_get_serial_sequence('permission', 'permission_id'), (SELECT max(permission_id) FROM permission));
SELECT setval(pg_get_serial_sequence('point',      'point_id'),      (SELECT max(point_id)      FROM point));
SELECT setval(pg_get_serial_sequence('category',   'category_id'),   (SELECT max(category_id)   FROM category));
SELECT setval(pg_get_serial_sequence('item',       'item_id'),       (SELECT max(item_id)       FROM item));
SELECT setval(pg_get_serial_sequence('warehouse',  'warehouse_id'),  (SELECT max(warehouse_id)  FROM warehouse));
SELECT setval(pg_get_serial_sequence('courier',    'courier_id'),    (SELECT max(courier_id)    FROM courier));
--rollback DELETE FROM courier WHERE mc_nickname = 'courier_bot';
--rollback DELETE FROM stock_item;
--rollback DELETE FROM warehouse WHERE name = 'Главный склад';
--rollback DELETE FROM item WHERE mc_material = 'DIAMOND';
--rollback DELETE FROM category WHERE name = 'Ресурсы';
--rollback DELETE FROM point WHERE label = 'Главный склад';
--rollback DELETE FROM user_role;
--rollback DELETE FROM role_permission;
--rollback DELETE FROM permission;
--rollback DELETE FROM role WHERE name IN ('BUYER', 'SELLER', 'ADMIN');
--rollback DELETE FROM app_user WHERE nickname IN ('buyer', 'seller', 'admin');